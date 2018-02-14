/* KMEANS MapReduce
This program performs KMeans clustering on a set of datapoints in parallel using the MapReduce paradigm.
This implementation is a reflection of the high level implementation found in the O'Reilly Book "Data Algorithms:
Recipes for Scaling Up with MapReduce and Spark"
File I/O was done following the tutorial code by Thomas Jungblut from the blog post found at:
http://codingwiththomas.blogspot.com/2011/05/k-means-clustering-with-mapreduce.html

This code is written for comparison purposes with the Multi-Agent Spatial Simulation library implementation of KMeans
clustering (entitled Best Tool Algorithm) and will be referenced in the Capstone Project presented by Collin Gordon
in the Spring Quarter 2018.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Created by colntrev on 2/13/18.
 */
public class KMeans {
    public static class KMeansMap extends Mapper<PointVector,PointVector,PointVector,PointVector>{
        private final List<PointVector> centers = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            Path cents = new Path(conf.get("centroid.path"));
            FileSystem fs = FileSystem.get(conf);
            try(SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(cents))){
                PointVector key = new PointVector();
                IntWritable value = new IntWritable();
                while(reader.next(key,value)){
                    PointVector clusterCenter = new PointVector(key);
                    centers.add(clusterCenter);
                }
            }
        }

        @Override
        public void map(PointVector cluster, PointVector point, Context context) throws IOException, InterruptedException {
            PointVector nearest = null;
            double nearestDistance = Double.MAX_VALUE;
            for(PointVector pv :centers){
                double distance = 0.0;
                if(nearest == null){
                    nearest = pv;
                    nearestDistance = distance;
                } else {
                    if(nearestDistance > distance){
                        nearest = pv;
                        nearestDistance = distance;
                    }
                }
            }
            context.write(nearest, point);
        }

    }

    public static class KMeansReduce extends Reducer<PointVector, PointVector, PointVector, PointVector> {
        private final List<PointVector> centers = new ArrayList<>();
        @Override
        protected void reduce(PointVector center, Iterable<PointVector> points, Context context) throws IOException,InterruptedException{
            List<PointVector> pointVectorList = new ArrayList<>();

        }

        @SuppressWarnings("deprecation")
        @Override
        public void cleanup(Context context) throws IOException,InterruptedException{
            super.cleanup(context);
            Configuration conf = context.getConfiguration();
            Path outPath = new Path(conf.get("centroid.path"));
            FileSystem fs = FileSystem.get(conf);
            fs.delete(outPath, true);
            try(SequenceFile.Writer out = SequenceFile.createWriter(fs, context.getConfiguration(),outPath,
                    PointVector.class, IntWritable.class)){
                final IntWritable value = new IntWritable(0);
                for(PointVector center : centers){
                    out.append(center, value);
                }
            }

        }
    }
    public static void main(String[] args){

    }
}
