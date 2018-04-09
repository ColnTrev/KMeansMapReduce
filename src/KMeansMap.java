import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by colntrev on 2/18/18.
 */
public class KMeansMap extends Mapper<CenterVector,PointVector,CenterVector,PointVector> {
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
    protected void map(CenterVector cluster, PointVector point, Context context) throws IOException, InterruptedException {
        CenterVector nearest = null;
        double nearestDistance = Double.MAX_VALUE;
        for(CenterVector c :centers){
            double dist = distance(c.getCenterVector(), point.getPointVector());
            if(nearest == null){
                nearest = c;
                nearestDistance = dist;
            } else {
                if(dist < nearestDistance){
                    nearest = c;
                    nearestDistance = dist;
                }
            }
        }
        context.write(nearest, point);
    }

    protected double distance(double[] center, double[] point){
        if(center.length != point.length){
            System.err.println("Error: dimensions not the same");
            System.exit(-1);
        }
        double sum = 0.0;
        for(int i = 0; i < center.length; i++){
            sum += center[i] * point[i];
        }
        return Math.sqrt(sum);
    }

}
