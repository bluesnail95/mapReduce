package recordReader;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import redis.clients.jedis.Jedis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;

/**
 * @Author bluesnail95
 * @Date 2019/8/10 9:34
 * @Description
 */
public class PartitionPruningOutputDriver {

    public static class RedisKey implements WritableComparable<RedisKey> {

        private int lastAccessMonth = 0;
        private Text field = new Text();

        public int getLastAccessMonth() {
            return lastAccessMonth;
        }

        public void setLastAccessMonth(int lastAccessMonth) {
            this.lastAccessMonth = lastAccessMonth;
        }

        public Text getField() {
            return field;
        }

        public void setField(String field) {
            this.field.set(field);
        }

        public int compareTo(RedisKey redisKey) {
            if(this.lastAccessMonth == redisKey.getLastAccessMonth()) {
                return this.field.compareTo(redisKey.getField());
            }else{
                return this.lastAccessMonth < redisKey.getLastAccessMonth() ? -1 : 1;
            }
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(lastAccessMonth);
            this.field.write(dataOutput);
        }

        public void readFields(DataInput dataInput) throws IOException {
            this.lastAccessMonth = dataInput.readInt();
            this.field.readFields(dataInput);
        }

        @Override
        public String toString() {
            return "lastAccessMonth=" + lastAccessMonth +", field=" + field ;
        }

        public int hashCode() {
            return toString().hashCode();
        }
    }

    public static class RedisLastAccessOutputFormat extends OutputFormat<RedisKey, Text> {

        public RecordWriter<RedisKey, Text> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            return new RedisLastAccessRecordWriter();
        }

        public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        }

        public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            return (new NullOutputFormat<Text,Text>()).getOutputCommitter(taskAttemptContext);
        }
    }

    public static class RedisLastAccessRecordWriter extends RecordWriter<RedisKey,Text> {

        private HashMap<Integer,Jedis> jedisHashMap = new HashMap<Integer,Jedis>();

        public RedisLastAccessRecordWriter () {
            Jedis jedis = new Jedis("127.0.0.1");
            jedis.connect();
            jedisHashMap.put(0,jedis);
        }

        public void write(RedisKey redisKey, Text value) throws IOException, InterruptedException {
            Jedis jedis = jedisHashMap.get(0);
            jedis.hset("month",redisKey.getField().toString(),value.toString());
        }

        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            for(Jedis jedis:jedisHashMap.values()) {
                jedis.disconnect();
            }
        }
    }

    public static class RedisLastAccessOutputMapper extends Mapper<Object,Text,RedisKey,Text> {

        public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        private RedisKey outKey = new RedisKey();
        private Text outValue = new Text();

        public void map(Object key,Text value,Context context) {
            JSONObject valueJson = JSONObject.parseObject(value.toString());
            String userId = valueJson.getString("userId");
            String reputation = valueJson.getString("reputation");
            String date = valueJson.getString("lastAccessDate");
            Calendar calendar = Calendar.getInstance();
            try {
                calendar.setTime(sdf.parse(date));
                outKey.setField(userId);
                outKey.setLastAccessMonth(calendar.get(Calendar.MONTH));
                outValue.set(reputation);
                context.write(outKey,outValue);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration,"Partition Pruning Output");
            job.setJarByClass(PartitionPruningOutputDriver.class);

            job.setMapperClass(RedisLastAccessOutputMapper.class);
            job.setNumReduceTasks(0);

            job.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.setInputPaths(job,new Path(args[0]));

            job.setOutputFormatClass(RedisLastAccessOutputFormat.class);

            job.setOutputValueClass(Text.class);
            job.setOutputKeyClass(RedisKey.class);

            int code = job.waitForCompletion(true) ? 0 :2;
            System.exit(code);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
