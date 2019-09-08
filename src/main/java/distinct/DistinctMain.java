package distinct;

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

/**
 * @Author bluesnail95
 * @Date 2019/7/20 17:09
 * @Description
 */
public class DistinctMain {

    public static class DistinctMapper extends Mapper<Object, Text,Text, NullWritable> {

        public void map(Object key,Text value,Context context) {
            try {
                context.write(value, NullWritable.get());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class DistinctReducer extends Reducer<Text, NullWritable,Text, NullWritable> {

        public void reduce(Text key,Iterable<NullWritable> values,Context context) {
            try {
                context.write(key,NullWritable.get());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Distinct");
            //与自己定义的类名保持一致
            job.setJarByClass(DistinctMapper.class);
            //与自己定义的Mapper类和Reducer类保持一致
            job.setMapperClass(DistinctMapper.class);
            job.setReducerClass(DistinctReducer.class);
            //设置的输出键和输出值和mapper定义的需要保持一致。
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
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
