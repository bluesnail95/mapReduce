package basejoinchaining;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author bluesnail95
 * @Date 2019/7/29 21:41
 * @Description
 */
public class UserIdCountMapper extends Mapper<Object,Text,Text,LongWritable> {

    public static final String RECORDS_COUNTER_NAME = "Records";
    public static final String AVERAGE_CALC_GROUP = "Average";

    private static final LongWritable ONE = new LongWritable(1);

    private Text outkey = new Text();

    public void map(Object key,Text value,Context context) {
        JSONObject valueJson = JSONObject.parseObject(value.toString());
        String userId = valueJson.getString("userId");
        outkey.set(userId);
        try {
            context.write(outkey,ONE);
            context.getCounter(AVERAGE_CALC_GROUP,RECORDS_COUNTER_NAME).increment(1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
