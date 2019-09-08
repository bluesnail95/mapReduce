package numericalSummarization;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author bluesnail95
 * @Date 2019/7/16 6:33
 * @Description
 */
public class MedianStdDevTuple implements Writable {

    private float median;

    private float stdDev;

    public float getMedian() {
        return median;
    }

    public void setMedian(float median) {
        this.median = median;
    }

    public float getStdDev() {
        return stdDev;
    }

    public void setStdDev(float stdDev) {
        this.stdDev = stdDev;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(median);
        dataOutput.writeFloat(stdDev);
    }

    public void readFields(DataInput dataInput) throws IOException {
        median = dataInput.readFloat();
        stdDev = dataInput.readFloat();
    }

    @Override
    public String toString() {
        return "{" +
                "median=" + median +
                ", stdDev=" + stdDev +
                '}';
    }
}
