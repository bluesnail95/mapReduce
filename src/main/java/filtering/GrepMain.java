package filtering;

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

import java.io.File;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author bluesnail95
 * @Date 2019/7/20 7:46
 * @Description
 */
public class GrepMain {

    public static class GrepMapper extends Mapper<Object, Text, NullWritable,Text> {
        private String matchGrep = null;

        public void map(Object key,Text value,Context context) {
            matchGrep = context.getConfiguration().get("matchGrep");
            Pattern pattern = Pattern.compile(matchGrep);
            Matcher matcher = pattern.matcher(value.toString());
            if(matcher.matches()) {
                try {
                    context.write(NullWritable.get(), value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static class GrepReducer extends Reducer<NullWritable,Text,NullWritable,Text> {
        private Random random = new Random();
        private Double percentage;

        public void reduce(NullWritable key,Iterable<Text> values,Context context) {
            String strPercentage = context.getConfiguration().get("filter_percentage");
            percentage = Double.valueOf(strPercentage);

            for(Text value:values) {
                double rand = random.nextDouble();
                if(rand < percentage) {
                    try {
                        context.write(NullWritable.get(), value);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            conf.set("matchGrep","^[a-zA-Z]+$");
            conf.setDouble("filter_percentage",0.5);
            Job job = Job.getInstance(conf, "Grep");
            //与自己定义的类名保持一致
            job.setJarByClass(GrepMain.class);
            //与自己定义的Mapper类和Reducer类保持一致
            job.setMapperClass(GrepMapper.class);
            job.setCombinerClass(GrepReducer.class);
            job.setReducerClass(GrepReducer.class);
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
