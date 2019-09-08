package numericalSummarization;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author bluesnail95
 * @Date 2019/7/14 21:36
 * @Description
 */
public class CountAverageTuple implements Writable {

    //计数
    private long count;

    //平均值
    private float average;

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public float getAverage() {
        return average;
    }

    public void setAverage(float average) {
        this.average = average;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(count);
        dataOutput.writeFloat(average);
    }

    public void readFields(DataInput dataInput) throws IOException {
        count = dataInput.readLong();
        average = dataInput.readFloat();
    }

    @Override
    public String toString() {
        return "{" +
                "count=" + count +
                ", average=" + average +
                '}';
    }
}
