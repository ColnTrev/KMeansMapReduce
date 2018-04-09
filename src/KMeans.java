/* KMEANS MapReduce
This program performs KMeans clustering on a set of datapoints in parallel using the MapReduce paradigm.

This implementation is a reflection of the high level implementation found in the O'Reilly Book "Data Algorithms:
Recipes for Scaling Up with MapReduce and Spark"

File I/O was done following the KMeans Clustering tutorial code by Thomas Jungblut from the blog post found at:
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class KMeans {

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException{
        int iter = 1;
        Configuration conf = new Configuration();
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        Path centroids = new Path(args[2]);
        conf.set("centroid.path", centroids.toString());
        conf.set("cycles", iter + "");

        Job job = Job.getInstance(conf);
        job.setJobName("KMeans Clustering");
        job.setJarByClass(KMeans.class);
        job.setMapperClass(KMeansMap.class);
        job.setReducerClass(KMeansReduce.class);

        FileInputFormat.addInputPath(job, in);
        FileSystem fs = FileSystem.get(conf);
        checkFileExists(fs,out,in,centroids);

        writeCenterFile(conf,centroids,fs);
        writeVectorFile(conf,in,fs);

        FileOutputFormat.setOutputPath(job,out);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(CenterVector.class);
        job.setOutputValueClass(PointVector.class);

        job.waitForCompletion(true); //controls recursion and prevents crashes

        long update = job.getCounters().findCounter(KMeansReduce.Counter.UPDATED).getValue();
        iter++;
        while(update > 0){ // we continue to loop until updated is 0 meaning our centers have converged and the algorithm terminates
            conf = new Configuration();

            conf.set("centroid.path", centroids.toString());
            conf.set("cycles", iter + "");

            job = Job.getInstance(conf);
            job.setJobName("KMeans Clustering " + iter);
            job.setJarByClass(KMeans.class);
            job.setMapperClass(KMeansMap.class);
            job.setReducerClass(KMeansReduce.class);

            in = new Path("clustering/intermediary_" + (iter - 1) + "/");
            out = new Path("clustering/intermediary_" + iter + "/");

            FileInputFormat.addInputPath(job, in);

            checkFileExists(fs, out);

            FileOutputFormat.setOutputPath(job,out);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            job.setOutputKeyClass(CenterVector.class);
            job.setOutputValueClass(PointVector.class);

            job.waitForCompletion(true);
            iter++;
            update = job.getCounters().findCounter(KMeansReduce.Counter.UPDATED).getValue();

        }

    }

    public static void checkFileExists(FileSystem fs, Path in, Path out, Path c) throws IOException {
        if(fs.exists(in)){
            fs.delete(in,true);
        }

        if(fs.exists(c)){
            fs.delete(c, true);
        }

        if(fs.exists(out)){
            fs.delete(out, true);
        }
    }

    public static void checkFileExists(FileSystem fs, Path out) throws IOException {
        if(fs.exists(out)){
            fs.delete(out, true);
        }
    }

    @SuppressWarnings("deprecation")
    public static void writeVectorFile(Configuration conf, Path in, FileSystem fs) throws IOException{
        try(SequenceFile.Writer vectorFile = SequenceFile.createWriter(fs,conf,in,CenterVector.class,PointVector.class)){
            vectorFile.append(new CenterVector(new PointVector(0,0)), new PointVector(3,4));
            vectorFile.append(new CenterVector(new PointVector(0,0)), new PointVector(5,6));
            vectorFile.append(new CenterVector(new PointVector(0,0)), new PointVector(1,8));
            vectorFile.append(new CenterVector(new PointVector(0,0)), new PointVector(2,7));
            vectorFile.append(new CenterVector(new PointVector(0,0)), new PointVector(4,4));
        }
    }

    @SuppressWarnings("deprecation")
    public static void writeCenterFile(Configuration conf, Path in, FileSystem fs) throws IOException {
        try(SequenceFile.Writer centerFile = SequenceFile.createWriter(fs, conf, in,
                CenterVector.class, IntWritable.class)){
            final IntWritable value = new IntWritable(0);
            centerFile.append(new CenterVector(1,1), value);
            centerFile.append(new CenterVector(2,4), value);
        }
    }
}
