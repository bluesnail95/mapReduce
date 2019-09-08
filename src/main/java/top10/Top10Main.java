package top10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.TreeMap;

/**
 * @Author bluesnail95
 * @Date 2019/7/20 17:09
 * @Description
 */
public class Top10Main {

    public static class Top10Mapper extends Mapper<Object, Text, NullWritable,Text> {

        private TreeMap<Integer,Text> sortedMap = new TreeMap<Integer,Text>();

        public void map(Object key,Text value,Context context) {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                Top10Data top10Data = objectMapper.readValue(value.toString(),Top10Data.class);
                Integer reputation = top10Data.getReputation();
                String userId = top10Data.getUserId();
                sortedMap.put(reputation,new Text(value));
                if(sortedMap.size() > 10) {
                    sortedMap.remove(sortedMap.firstKey());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        protected void cleanup(Context context) {
            for(Text t:sortedMap.values()) {
                try {
                    context.write(NullWritable.get(),t);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static class Top10Reducer extends Reducer<NullWritable,Text,NullWritable,Text> {

        private TreeMap<Integer,Text> sortedMap = new TreeMap<Integer,Text>();

        public void reduce(NullWritable key,Iterable<Text> values,Context context) {
            for(Text value:values) {
                System.out.println(value.toString());
                ObjectMapper objectMapper = new ObjectMapper();
                try {
                    Top10Data top10Data = objectMapper.readValue(value.toString(), Top10Data.class);
                    int reputation = top10Data.getReputation();
                    String userId = top10Data.getUserId();
                    sortedMap.put(reputation,new Text(value));

                    if(sortedMap.size() > 10) {
                        sortedMap.remove(sortedMap.firstKey());
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            for (Text t:sortedMap.values()) {
                try {
                    context.write(NullWritable.get(), t);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Top 10");
            //与自己定义的类名保持一致
            job.setJarByClass(Top10Main.class);
            //与自己定义的Mapper类和Reducer类保持一致
            job.setMapperClass(Top10Mapper.class);
            job.setReducerClass(Top10Reducer.class);
            //设置的输出键和输出值和mapper定义的需要保持一致。
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            //输入输出路径
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileUtil.fullyDelete(new File(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true)?0:1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
