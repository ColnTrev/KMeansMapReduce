import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by colntrev on 2/13/18.
 */
public class PointVector implements WritableComparable<PointVector> {
    private double[] point;

    public PointVector(){
        super();
        point = new double[]{0,0};
    }
    public PointVector(double x, double y){
        point = new double[]{x,y};
    }
    public PointVector(PointVector pv){
        super();
        int size = pv.point.length;
        this.point = new double[size];
        System.arraycopy(pv.point,0,this.point, 0, size);
    }
    public double[] getPointVector(){
        return point;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(point.length);
        for(int i = 0; i < point.length; i++){
            dataOutput.writeDouble(point[i]);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        point = new double[size];
        for(int i = 0; i < point.length; i++){
            point[i] = dataInput.readDouble();
        }
    }

    @Override
    public int compareTo(PointVector pv){

        return 0;
    }
}
