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
import java.util.List;


/**
 * Created by colntrev on 2/13/18.
 */
public class KMeans {
    public static class KMeansMap extends Mapper<CenterVector,PointVector,CenterVector,PointVector>{
        private final List<CenterVector> centers = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            Path cents = new Path(conf.get("centroid.path"));
            FileSystem fs = FileSystem.get(conf);
            try(SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(cents))){
                CenterVector key = new CenterVector();
                IntWritable value = new IntWritable();
                while(reader.next(key,value)){
                    CenterVector clusterCenter = new CenterVector(key);
                    centers.add(clusterCenter);
                }
            }
        }

        @Override
        public void map(CenterVector cluster, PointVector point, Context context) throws IOException, InterruptedException {
            CenterVector nearest = null;
            double nearestDistance = Double.MAX_VALUE;
            for(CenterVector pv :centers){
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

    public static class KMeansReduce extends Reducer<CenterVector, PointVector, CenterVector, PointVector> {
        private final List<CenterVector> centers = new ArrayList<>();
        @Override
        protected void reduce(CenterVector center, Iterable<PointVector> points, Context context) throws IOException,InterruptedException{
            List<PointVector> pointVectorList = new ArrayList<>();
            CenterVector newCenter = null;
            for(PointVector point : points){
                pointVectorList.add(new PointVector(point));
                if(newCenter == null){
                    newCenter = new CenterVector(point);
                } else {
                    newCenter.add(point.getPointVector());
                }
            }
            newCenter.mean(pointVectorList.size());
            centers.add(new CenterVector(newCenter));

            for(PointVector pv : pointVectorList){
                context.write(newCenter, pv);
            }
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
                for(CenterVector center : centers){
                    out.append(center, value);
                }
            }

        }
    }
    public static void main(String[] args){

    }
}
