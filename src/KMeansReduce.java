import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by colntrev on 2/18/18.
 */

public class KMeansReduce extends Reducer<CenterVector, PointVector, CenterVector, PointVector> {

    // This Counter reduces the recursion created by the MapReduce paradigm and prevents crashes
    public static enum Counter {
        CONVERGENCE
    }

    private final List<CenterVector> centers = new ArrayList<>();

    @Override
    protected void reduce(CenterVector centroid, Iterable<PointVector> points, Context context) throws IOException,InterruptedException{
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
        CenterVector updated = new CenterVector(newCenter);
        centers.add(updated);

        for(PointVector pv : pointVectorList){
            context.write(updated, pv);
        }
        if(updated.converged(centroid)){
            context.getCounter(Counter.CONVERGENCE).increment(1);
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
