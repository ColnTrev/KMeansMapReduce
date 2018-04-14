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
import java.util.Random;

public class KMeans {

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException{
        int iter = 1;
        int maxPoints = 100;
        int maxX = 30;
        int maxY = 30;
        int k = 3;
        Configuration conf = new Configuration();
        Path in = new Path("files/clustering/import/data");
        Path out = new Path("files/clustering/depth_1");
        Path centroids = new Path("files/clustering/import/center/cen.seq");
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

        writeCenterFile(conf,centroids,fs, maxX, maxY, k);
        writeVectorFile(conf,in,fs, maxPoints, maxX, maxY);

        FileOutputFormat.setOutputPath(job,out);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(CenterVector.class);
        job.setOutputValueClass(PointVector.class);
        long startTime = System.currentTimeMillis();
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

            in = new Path("files/clustering/depth_" + (iter - 1) + "/");
            out = new Path("files/clustering/depth_" + iter + "/");

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
         long endTime = System.currentTimeMillis();
        System.out.println("Elapsed Time: " + (endTime - startTime));
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
    public static void writeVectorFile(Configuration conf, Path in, FileSystem fs, int maxPoints, int maxX, int maxY) throws IOException{
        Random rand = new Random();
        try(SequenceFile.Writer vectorFile = SequenceFile.createWriter(fs,conf,in,CenterVector.class,PointVector.class)){
            for(int i = 0; i < maxPoints; i++){
                int x = rand.nextInt(maxX);
                int y = rand.nextInt(maxY);
                vectorFile.append(new CenterVector(new PointVector(0,0)), new PointVector(x,y));
            }
        }
    }

    @SuppressWarnings("deprecation")
    public static void writeCenterFile(Configuration conf, Path in, FileSystem fs, int maxX, int maxY, int k) throws IOException {
        Random rand = new Random();
        try(SequenceFile.Writer centerFile = SequenceFile.createWriter(fs, conf, in,
                CenterVector.class, IntWritable.class)){
            final IntWritable value = new IntWritable(0);
            for(int i = 0; i < k; i++){
                int x = rand.nextInt(maxX);
                int y = rand.nextInt(maxY);
                centerFile.append(new CenterVector(x,y), value);
            }
        }
    }
}
