import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
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
                while(reader.next(key)){
                    PointVector clusterCenter = new PointVector(key);
                    centers.add(clusterCenter);
                }
            }
        }
    }
    public static void main(String[] args){

    }
}
