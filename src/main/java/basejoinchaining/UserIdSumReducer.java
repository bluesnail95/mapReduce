package basejoinchaining;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author bluesnail95
 * @Date 2019/7/29 21:55
 * @Description
 */
public class UserIdSumReducer extends Reducer<Text, LongWritable,Text,LongWritable> {

    public static final String USERS_COUNTER_NAME = "Users";
    public static final String AVERAGE_CALC_GROUP = "Average";
    public static final LongWritable outValue = new LongWritable();

    public void reduce(Text key,Iterable<LongWritable> values,Context context) {
        context.getCounter(AVERAGE_CALC_GROUP,USERS_COUNTER_NAME).increment(1);
        int sum = 0;
        for(LongWritable value:values) {
            sum += value.get();
        }
        outValue.set(sum);
        try {
            context.write(key,outValue);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
