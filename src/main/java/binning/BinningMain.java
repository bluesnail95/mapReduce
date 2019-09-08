package binning;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;

/**
 * @Author bluesnail95
 * @Date 2019/7/22 20:46
 * @Description
 */
public class BinningMain {

    public static class BinningMapper extends Mapper<Object,Text, Text, NullWritable> {

        private MultipleOutputs<Text,NullWritable> output = null;

        public void setup(Context context) {
            output = new MultipleOutputs<Text, NullWritable>(context);
        }

        public void map(Object key,Text value,Context context) {
            try {
                JSONObject valueJson = JSONObject.parseObject(value.toString());
                String tag = valueJson.getString("tag");
                if(StringUtils.isBlank(tag)) {
                    return;
                }
                if(tag.equalsIgnoreCase("hadoop")) {
                    output.write("bins",value,NullWritable.get(),"hadoop-tag");
                }
                if(tag.equalsIgnoreCase("hive")) {
                    output.write("bins",value,NullWritable.get(),"hive-tag");
                }
                if(tag.equalsIgnoreCase("hbase")) {
                    output.write("bins",value,NullWritable.get(),"hbase-tag");
                }
                if(tag.equalsIgnoreCase("pig")) {
                    output.write("bins",value,NullWritable.get(),"pig-tag");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        protected void cleanup(Context context) {
            try {
                output.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Binning");
            //与自己定义的类名保持一致
            job.setJarByClass(BinningMain.class);
            //与自己定义的Mapper类和Reducer类保持一致
            job.setMapperClass(BinningMapper.class);
            //设置的输出键和输出值和mapper定义的需要保持一致。
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            //输入输出路径
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileUtil.fullyDelete(new File(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class,Text.class,NullWritable.class);
            MultipleOutputs.setCountersEnabled(job,true);
            job.setNumReduceTasks(0);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
