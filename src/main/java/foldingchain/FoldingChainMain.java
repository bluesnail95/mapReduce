package foldingchain;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.lib.NullOutputFormat;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;

/**
 * @Author bluesnail95
 * @Date 2019/8/3 9:58
 * @Description
 */
public class FoldingChainMain {

    public static class UserIdCountMapper extends MapReduceBase implements Mapper<Object,Text,Text,LongWritable> {

        private static final LongWritable one = new LongWritable(1);
        private Text outKey = new Text();

        public void map(Object o, Text text, OutputCollector<Text, LongWritable> outputCollector, Reporter reporter) throws IOException {
            JSONObject valueJson = JSONObject.parseObject(text.toString());
            String userId = valueJson.getString("userId");
            outKey.set(userId);
            outputCollector.collect(outKey,one);
        }
    }

    public static class UserIdReputationEnrichmentMapper extends MapReduceBase implements Mapper<Text, LongWritable,Text,LongWritable> {
        private Text outKey = new Text();
        private HashMap<String,String> userIdReputation = new HashMap<String,String>();

        public void configure(JobConf jobConf) {
            try {
                BufferedReader bufferReader = new BufferedReader(new InputStreamReader(new FileInputStream(new File("foldingchain/1.txt"))));

                String line = null;
                while((line = bufferReader.readLine()) != null) {
                    JSONObject lineJson = JSONObject.parseObject(line);
                    userIdReputation.put(lineJson.getString("userId"),lineJson.getString("reputation"));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void map(Text key, LongWritable value, OutputCollector<Text, LongWritable> outputCollector, Reporter reporter) throws IOException {
            String reputation = userIdReputation.get(key.toString());
            if(reputation != null) {
                outKey.set(reputation);
                outputCollector.collect(outKey,value);
            }
        }

    }

    public static class LongSumReducer extends MapReduceBase implements Reducer<Text,LongWritable,Text,LongWritable> {

        private LongWritable outvalue = new LongWritable();

        public void reduce(Text key, Iterator<LongWritable> iterator, OutputCollector<Text, LongWritable> outputCollector, Reporter reporter) throws IOException {
            int sum = 0;
            while(iterator.hasNext()) {
                sum += iterator.next().get();
            }
            outvalue.set(sum);
            outputCollector.collect(key,outvalue);
        }
    }

    public static class UserIdBinningMapper extends MapReduceBase implements Mapper<Text,LongWritable,Text,LongWritable> {

        private MultipleOutputs mos = null;
        public void configure(JobConf conf) {
            mos = new MultipleOutputs(conf);
        }

        public void map(Text key, LongWritable value, OutputCollector<Text, LongWritable> outputCollector, Reporter reporter) throws IOException {
            try {
                if(Integer.parseInt(key.toString()) < 5000) {
                    mos.getCollector("below5000",reporter).collect(key,value);
                }else{
                    mos.getCollector("above5000",reporter).collect(key,value);
                }
            } catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void close() {
            try {
                mos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) {
        JobConf conf = new JobConf("FoldingChainMain");
        conf.setJarByClass(FoldingChainMain.class);

        Path postPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        ChainMapper.addMapper(conf,UserIdCountMapper.class, LongWritable.class,Text.class,Text.class,LongWritable.class,false,new JobConf(false));
        ChainMapper.addMapper(conf,UserIdReputationEnrichmentMapper.class,Text.class,LongWritable.class,Text.class,LongWritable.class,false,new JobConf(false));
        ChainReducer.setReducer(conf,LongSumReducer.class,Text.class,LongWritable.class,Text.class,LongWritable.class,false,new JobConf(false));
        ChainReducer.addMapper(conf,UserIdBinningMapper.class,Text.class,LongWritable.class,Text.class,LongWritable.class,false,new JobConf(false));

        conf.setCombinerClass(LongSumReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        TextInputFormat.setInputPaths(conf,postPath);

        conf.setOutputFormat(NullOutputFormat.class);
        FileOutputFormat.setOutputPath(conf,outputPath);
        MultipleOutputs.addNamedOutput(conf,"above5000",TextOutputFormat.class,Text.class,LongWritable.class);
        MultipleOutputs.addNamedOutput(conf,"below5000",TextOutputFormat.class,Text.class,LongWritable.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);
        try {
            RunningJob job = JobClient.runJob(conf);
            while(!job.isComplete()){
                Thread.sleep(5000);
            }
            System.exit(job.isSuccessful() ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
