package recordReader;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * @Author bluesnail95
 * @Date 2019/8/10 10:32
 * @Description
 */
public class PartitionPruningInputDriver {
    public static final String REDIS_SELECTED_MONTHS_CONF = "mapred.redisLastAccessInputFormat.months";
    private static final HashMap<String,Integer> MONTH_FROM_STRING = new HashMap<String,Integer>();
    private static final HashMap<String,String> MONTH_TO_INST_MAP = new HashMap<String, String>();

    public static class RedisLastAccessInputSplit extends InputSplit implements Writable {

        private String location = null;
        private List<String> hashKeys = new ArrayList<String>();

        public RedisLastAccessInputSplit(String location, List<String> hashKeys) {
            this.location = location;
            this.hashKeys = hashKeys;
        }

        public RedisLastAccessInputSplit() {}

        public RedisLastAccessInputSplit(String host) {
            this.location = host;
        }

        public void addHashKey(String key) {
            hashKeys.add(key);
        }

        public void removeHashKey(String key) {
            hashKeys.remove(key);
        }

        public List<String> getHashKeys() {
            return hashKeys;
        }

        public void write(DataOutput dataOutput) throws IOException {
            try {
                dataOutput.writeUTF(location);
                dataOutput.writeInt(hashKeys.size());
                for(String key:hashKeys){
                    dataOutput.writeUTF(key);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void readFields(DataInput dataInput) throws IOException {
            try {
                location = dataInput.readUTF();
                int numKeys = dataInput.readInt();
                hashKeys.clear();
                for (int i = 0; i < numKeys; i++) {
                    hashKeys.add(dataInput.readUTF());
                }
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

    public static class RedisLastAccessRecordReader extends RecordReader<PartitionPruningOutputDriver.RedisKey,Text> {

        private Iterator<Map.Entry<String,String>> hashIterator = null;
        private Map.Entry<String,String> currentEntry = null;
        private Iterator<String> hashKeys = null;
        private PartitionPruningOutputDriver.RedisKey redisKey = new PartitionPruningOutputDriver.RedisKey();
        private Text value = new Text();
        private String host = null;
        private int currentHashMonth = 0;
        private float processedKVs = 0,totalKVs = 0;

        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            host = inputSplit.getLocations()[0];
            hashKeys = ((RedisLastAccessInputSplit)inputSplit).getHashKeys().iterator();
        }

        public boolean nextKeyValue() throws IOException, InterruptedException {
            boolean nextHashKey = false;
            do{
                if(hashIterator == null || !hashIterator.hasNext()) {
                    if (!hashKeys.hasNext()) {
                        return false;
                    } else {
                        Jedis jedis = new Jedis(host);
                        jedis.connect();
                        String strKey = hashKeys.next();
                        currentHashMonth = MONTH_FROM_STRING.get(strKey);
                        hashIterator = jedis.hgetAll(strKey).entrySet().iterator();
                        jedis.disconnect();
                    }
                }

                if(hashIterator.hasNext()) {
                    currentEntry = hashIterator.next();
                    redisKey.setField(currentEntry.getKey());
                    redisKey.setLastAccessMonth(currentHashMonth);
                    value.set(currentEntry.getValue());
                }else{
                    nextHashKey = true;
                }
            }while(nextHashKey);
            return true;
        }

        public PartitionPruningOutputDriver.RedisKey getCurrentKey() throws IOException, InterruptedException {
            return redisKey;
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

    public static class RedisLastAccessInputFormat extends InputFormat<PartitionPruningOutputDriver.RedisKey, Text> {

        static {
            MONTH_TO_INST_MAP.put("1","127.0.0.1");
            MONTH_TO_INST_MAP.put("2","127.0.0.1");
            MONTH_TO_INST_MAP.put("3","127.0.0.1");
            MONTH_TO_INST_MAP.put("4","127.0.0.1");
            MONTH_TO_INST_MAP.put("5","127.0.0.1");
            MONTH_TO_INST_MAP.put("6","127.0.0.1");
            MONTH_TO_INST_MAP.put("7","127.0.0.1");
            MONTH_TO_INST_MAP.put("8","127.0.0.1");
            MONTH_TO_INST_MAP.put("9","127.0.0.1");
            MONTH_TO_INST_MAP.put("10","127.0.0.1");
            MONTH_TO_INST_MAP.put("11","127.0.0.1");
            MONTH_TO_INST_MAP.put("12","127.0.0.1");
            MONTH_FROM_STRING.put("1",1);
            MONTH_FROM_STRING.put("2",2);
            MONTH_FROM_STRING.put("3",3);
            MONTH_FROM_STRING.put("4",4);
            MONTH_FROM_STRING.put("5",5);
            MONTH_FROM_STRING.put("6",6);
            MONTH_FROM_STRING.put("7",7);
            MONTH_FROM_STRING.put("8",8);
            MONTH_FROM_STRING.put("9",9);
            MONTH_FROM_STRING.put("10",10);
            MONTH_FROM_STRING.put("11",11);
            MONTH_FROM_STRING.put("12",12);
        }

        public static void setRedisLastAccessMonths(Job job,String months) {
            job.getConfiguration().set(REDIS_SELECTED_MONTHS_CONF,months);
        }

        public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
            String months = jobContext.getConfiguration().get(REDIS_SELECTED_MONTHS_CONF);
            if(StringUtils.isBlank(months)) {
                throw new IOException(REDIS_SELECTED_MONTHS_CONF + "is null or empty");
            }
            HashMap<String,RedisLastAccessInputSplit> instanceToSplitMap = new HashMap<String,RedisLastAccessInputSplit>();
            for(String month:months.split(",")) {
                String host = MONTH_TO_INST_MAP.get(month);
                RedisLastAccessInputSplit redisLastAccessInputSplit = instanceToSplitMap.get(host);
                if(null == redisLastAccessInputSplit) {
                    redisLastAccessInputSplit = new RedisLastAccessInputSplit(host);
                    redisLastAccessInputSplit.addHashKey(month);
                    instanceToSplitMap.put(host,redisLastAccessInputSplit);
                }else{
                    redisLastAccessInputSplit.addHashKey(month);
                }
            }
            return new ArrayList<InputSplit>(instanceToSplitMap.values());
        }

        public RecordReader<PartitionPruningOutputDriver.RedisKey, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            return new RedisLastAccessRecordReader();
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration,"Partition Pruning Input");
            job.setJarByClass(PartitionPruningInputDriver.class);

            job.setNumReduceTasks(0);

            job.setInputFormatClass(RedisLastAccessInputFormat.class);
            RedisLastAccessInputFormat.setRedisLastAccessMonths(job,"1,2,3,4,5,6,7,8,9,10,11,12");

            job.setOutputFormatClass(TextOutputFormat.class);
            FileUtil.fullyDelete(new File(args[0]));
            TextOutputFormat.setOutputPath(job,new Path(args[0]));

            job.setOutputValueClass(Text.class);
            job.setOutputKeyClass(PartitionPruningOutputDriver.RedisKey.class);

            int code = job.waitForCompletion(true) ? 0 :2;
            System.exit(code);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
