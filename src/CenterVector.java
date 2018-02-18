/**
 * Created by colntrev on 2/16/18.
 */
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CenterVector implements WritableComparable<CenterVector> {
    private double[] point;

    public CenterVector(){
        super();
        point = new double[]{0,0};
    }

    public CenterVector(double x, double y){
        super();
        point = new double[]{x,y};
    }
    public CenterVector(CenterVector pv){
        super();
        int size = pv.point.length;
        this.point = new double[size];
        System.arraycopy(pv.point,0,this.point, 0, size);
    }
    public CenterVector(PointVector pv){
        super();
        double[] vec = pv.getPointVector();
        point = new double[vec.length];
        add(vec);
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
    public boolean converged(CenterVector c){
        return false;
    }
    public void add(double[] p){
        for(int i = 0; i < point.length; i++){
            point[i] += p[i];
        }
    }
    public void mean(int total){
        for(int i = 0; i < point.length; i++){
            point[i] /= total;
        }
    }

    @Override
    public int compareTo(CenterVector pv){

        return 0;
    }
}
