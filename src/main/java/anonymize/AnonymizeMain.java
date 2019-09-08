package anonymize;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import wordcount.CountNumUsersByStateMain;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

/**
 * @Author bluesnail95
 * @Date 2019/7/24 21:26
 * @Description
 */
public class AnonymizeMain {

    public static class AnnoymizeMapper extends Mapper<Object, Text, IntWritable, Text> {

        private Random random = new Random();

        public void map(Object key,Text value,Context context) {
            JSONObject valueJson = JSONObject.parseObject(value.toString());
            String lastAccessDate = valueJson.getString("lastAccessDate");
            lastAccessDate = lastAccessDate.substring(0, lastAccessDate.indexOf("T"));
            valueJson.put("lastAccessDate",lastAccessDate);
            valueJson.remove("userId");
            try {
                context.write(new IntWritable(random.nextInt()),new Text(valueJson.toString()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    public static class AnnoymizeReducer extends Reducer<IntWritable,Text,Text, NullWritable> {

        public void reduce(IntWritable key,Iterable<Text> values,Context context) {
            for(Text value:values) {
                try {
                    context.write(value,NullWritable.get());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration, "Annoymize");
            job.setJarByClass(AnonymizeMain.class);
            job.setMapperClass(AnnoymizeMapper.class);
            job.setReducerClass(AnnoymizeReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileUtil.fullyDelete(new File(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            int code = job.waitForCompletion(true )? 0 : 1;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
