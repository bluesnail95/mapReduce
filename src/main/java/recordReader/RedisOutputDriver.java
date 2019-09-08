package recordReader;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.HashMap;

/**
 * @Author bluesnail95
 * @Date 2019/8/9 20:24
 * @Description
 */
public class RedisOutputDriver {

    public static class RedisHashOutputFormat extends OutputFormat<Text, Text> {

        public static final String REDIS_HOSTS_CONF = "mapred.redishashoutputformat.hosts";
        public static final String REDIS_HASH_KEY_CONF = "mapred.redishashoutputformat.key";

        public static void setRedisHosts(Job job,String hosts){
            job.getConfiguration().set(REDIS_HOSTS_CONF,hosts);
        }

        public static void setRedisHashKey(Job job,String hashKey) {
            job.getConfiguration().set(REDIS_HASH_KEY_CONF,hashKey);
        }

        public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            return new RedisHashRecordWriter(taskAttemptContext.getConfiguration().get(REDIS_HASH_KEY_CONF),
                    taskAttemptContext.getConfiguration().get(REDIS_HOSTS_CONF));
        }

        public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
            String host = jobContext.getConfiguration().get(REDIS_HOSTS_CONF);
            if(StringUtils.isBlank(host)) {
                throw new IOException(REDIS_HOSTS_CONF + "is not set in configuration");
            }
            String hashKey = jobContext.getConfiguration().get(REDIS_HASH_KEY_CONF);
            if(StringUtils.isBlank(hashKey)) {
                throw new IOException(REDIS_HASH_KEY_CONF + "is not set in configuration");
            }
        }

        public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            return (new NullOutputFormat<Text,Text>()).getOutputCommitter(taskAttemptContext);
        }
    }

    public static class RedisHashRecordWriter extends RecordWriter<Text,Text> {

        private HashMap<Integer,Jedis> jedisMap = new HashMap<Integer,Jedis>();
        private String hashKey = null;

        public RedisHashRecordWriter(String hashKey,String hosts) {
            this.hashKey = hashKey;
            int index = 0;
            for(String host:hosts.split(",")) {
                try {
                    Jedis jedis = new Jedis(host);
                    jedis.connect();
                    jedisMap.put(index,jedis);
                    index++;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        public void write(Text key, Text value) throws IOException, InterruptedException {
            try {
                Jedis jedis = jedisMap.get(Math.abs(key.hashCode()) % jedisMap.size());
                jedis.hset(hashKey,key.toString(),value.toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            for(Jedis jedis:jedisMap.values()) {
                jedis.disconnect();
            }
        }
    }

    public static class RedisOutputMapper extends Mapper<Object,Text,Text,Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(Object key,Text value,Context context){
            JSONObject valueJson = JSONObject.parseObject(value.toString());
            String userId = valueJson.getString("userId");
            String reputation = valueJson.getString("reputation");
            outKey.set(userId);
            outValue.set(reputation);
            try {
                context.write(outKey,outValue);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args) {
        Path inputPath = new Path(args[0]);
        String hosts = args[1];
        String hashName = args[2];


        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration,"RedisOutputFormat");
            job.setJarByClass(RedisOutputDriver.class);

            job.setMapperClass(RedisOutputMapper.class);
            job.setNumReduceTasks(0);

            job.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.setInputPaths(job,inputPath);

            job.setOutputFormatClass(RedisHashOutputFormat.class);
            RedisHashOutputFormat.setRedisHashKey(job,hashName);
            RedisHashOutputFormat.setRedisHosts(job,hosts);

            job.setOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);

            int code = job.waitForCompletion(true) ? 0 :1;
            System.exit(code);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
