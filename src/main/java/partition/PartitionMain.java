package partition;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * @Author bluesnail95
 * @Date 2019/7/22 6:27
 * @Description
 */
public class PartitionMain {

    public static class PatitionMapper extends Mapper<Object,Text, IntWritable, Text> {

        private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

        public void map(Object key,Text value,Context context) {
            try {
                JSONObject valueJson = JSONObject.parseObject(value.toString());
                String strDate = valueJson.getString("lastAccessDate");
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(frmt.parse(strDate));
                int year = calendar.get(Calendar.YEAR);
                context.write(new IntWritable(year),value);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class LastAccessDatePartitioner extends Partitioner<IntWritable,Text> implements Configurable {

        private static final String MIN_LAST_ACCESS_DATE_YEAR = "min.last.access.date.year";
        private Configuration conf = null;
        private int minLastAccessDateYear = 0;

        public void setConf(Configuration conf) {
            this.conf = conf;
            minLastAccessDateYear = conf.getInt(MIN_LAST_ACCESS_DATE_YEAR,0);
        }

        public Configuration getConf() {
            return conf;
        }

        public int getPartition(IntWritable key, Text value, int numPartitions) {
            return key.get() - minLastAccessDateYear;
        }

        public static void setMinLastAccessDate(Job job,int minLastAccessDateYear) {
            job.getConfiguration().setInt(MIN_LAST_ACCESS_DATE_YEAR,minLastAccessDateYear);
        }
    }

    public static class PatitionReducer extends Reducer<IntWritable,Text,Text,NullWritable> {

        public void reduce(IntWritable key,Iterable<Text> values,Context context) {
            for(Text value:values) {
                try {
                    context.write(value, NullWritable.get());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Partition");
            //与自己定义的类名保持一致
            job.setJarByClass(PartitionMain.class);
            //与自己定义的Mapper类和Reducer类保持一致
            job.setMapperClass(PatitionMapper.class);
            job.setPartitionerClass(LastAccessDatePartitioner.class);
            LastAccessDatePartitioner.setMinLastAccessDate(job,2010);
            job.setNumReduceTasks(10);
            job.setReducerClass(PatitionReducer.class);
            //设置的输出键和输出值和mapper定义的需要保持一致。
            job.setOutputKeyClass(IntWritable.class);
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
