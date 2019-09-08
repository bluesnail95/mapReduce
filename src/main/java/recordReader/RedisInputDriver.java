package recordReader;

import com.alibaba.fastjson.JSONObject;
import file.FileUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @Author bluesnail95
 * @Date 2019/8/9 22:02
 * @Description
 */
public class RedisInputDriver {

    public static class RedisHashInputSplit extends InputSplit implements Writable {

        private String location = null;
        private String hashKey = null;

        public RedisHashInputSplit(String host,String hashKey) {
            this.location = host;
            this.hashKey = hashKey;
        }

        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            this.location = location;
        }

        public String getHashKey() {
            return hashKey;
        }

        public void setHashKey(String hashKey) {
            this.hashKey = hashKey;
        }

        public void write(DataOutput dataOutput) throws IOException {
            try {
                dataOutput.writeUTF(location);
                dataOutput.writeUTF(hashKey);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void readFields(DataInput dataInput) throws IOException {
            try {
                this.location = dataInput.readUTF();
                this.hashKey = dataInput.readUTF();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public long getLength() throws IOException, InterruptedException {
            return 0;
        }

        public String[] getLocations() throws IOException, InterruptedException {
            return new String[]{location};
        }
    }

    public static class RedisHashInputFormat extends InputFormat<Text,Text> {

        public static final String REDIS_HOSTS_CONF = "mapred.redishashinputformat.hosts";
        public static final String REDIS_HASH_KEY_CONF = "mapred.redishashinputformat.key";
        private static final Logger log = Logger.getLogger(RedisHashInputFormat.class);

        public static void setRedisHosts(Job job,String hosts){
            job.getConfiguration().set(REDIS_HOSTS_CONF,hosts);
        }

        public static void setRedisHashKey(Job job,String hashKey) {
            job.getConfiguration().set(REDIS_HASH_KEY_CONF,hashKey);
        }

        public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
            try {
                String hosts = jobContext.getConfiguration().get(REDIS_HOSTS_CONF);
                if(StringUtils.isBlank(hosts)) {
                    throw new IOException(REDIS_HOSTS_CONF + "is not set in configuration");
                }
                String hashKey = jobContext.getConfiguration().get(REDIS_HASH_KEY_CONF);
                if(StringUtils.isBlank(hashKey)) {
                    throw new IOException(REDIS_HASH_KEY_CONF + "is not set in configuration");
                }
                List<InputSplit> splits = new ArrayList<InputSplit>();
                for(String host:hosts.split(",")) {
                    splits.add(new RedisHashInputSplit(host,hashKey));
                }
                return splits;
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }

        public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            RecordReader recordReader = null;
            try {
                recordReader = new RedisHashRecordReader();
                recordReader.initialize(inputSplit,taskAttemptContext);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return recordReader;
        }
    }

    public static class RedisHashRecordReader extends RecordReader<Text,Text> {

        private static final Logger log = Logger.getLogger(RedisHashRecordReader.class);
        private Iterator<Map.Entry<String,String>> keyValueMapIter = null;
        private Text key = new Text(), value = new Text();
        private float processedKVs = 0,totalKVs = 0;
        private Map.Entry<String,String> currentEntry = null;

        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            try {
                String host = inputSplit.getLocations()[0];
                String hashKey = ((RedisHashInputSplit)inputSplit).getHashKey();

                Jedis jedis = new Jedis(host);
                jedis.connect();
                jedis.getClient().setTimeoutInfinite();
                totalKVs = jedis.hlen(hashKey);
                keyValueMapIter = jedis.hgetAll(hashKey).entrySet().iterator();
                jedis.disconnect();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public boolean nextKeyValue() throws IOException, InterruptedException {
            try {
                while(keyValueMapIter.hasNext()) {
                    currentEntry = keyValueMapIter.next();
                    key.set(currentEntry.getKey());
                    value.set(currentEntry.getValue());
                    processedKVs++;
                    return true;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return false;
        }

        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        public float getProgress() throws IOException, InterruptedException {
            return processedKVs/totalKVs;
        }

        public void close() throws IOException {

        }
    }

    public static void main(String[] args) {
        Path outputPath = new Path(args[0]);
        String hosts = args[1];
        String hashName = args[2];


        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration,"Redis Input");
            job.setJarByClass(RedisInputDriver.class);
            job.setNumReduceTasks(0);

            job.setInputFormatClass(RedisHashInputFormat.class);
            RedisHashInputFormat.setRedisHashKey(job,hashName);
            RedisHashInputFormat.setRedisHosts(job,hosts);

            job.setOutputFormatClass(TextOutputFormat.class);
            FileUtil.deleteFile(args[2]);
            TextOutputFormat.setOutputPath(job,outputPath);

            job.setOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);

            int code = job.waitForCompletion(true) ? 0 :3;
            System.exit(code);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
