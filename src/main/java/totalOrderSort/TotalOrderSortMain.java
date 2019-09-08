package totalOrderSort;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.File;

/**
 * @Author bluesnail95
 * @Date 2019/7/23 6:25
 * @Description
 */
public class TotalOrderSortMain {

    public static class LastAccessDateMapper extends Mapper<Object,Text,Text,Text> {

        public void map(Object key, Text value, Context context) {
            JSONObject valueJson = JSONObject.parseObject(value.toString());
            String lastAccessDate = valueJson.getString("lastAccessDate");
            try {
                context.write(new Text(lastAccessDate),value);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class ValueReducer extends Reducer<Text,Text,Text, NullWritable> {

        public void reduce(Text key,Iterable<Text> values,Context context) {
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
        Path inputPath = new Path(args[0]);
        Path partitionFile = new Path(args[1] + "_partitions.lst");
        Path outputStage = new Path(args[1] + "_staging");
        Path outputOrder = new Path(args[1]);

        try {
            Job sampleJob = Job.getInstance(configuration,"TotalOrderSortingStage");
            sampleJob.setJarByClass(TotalOrderSortMain.class);
            sampleJob.setMapperClass(LastAccessDateMapper.class);
            sampleJob.setNumReduceTasks(0);
            sampleJob.setOutputKeyClass(Text.class);
            sampleJob.setOutputValueClass(Text.class);
            TextInputFormat.setInputPaths(sampleJob,inputPath);
            sampleJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileUtil.fullyDelete(new File(args[1] + "_staging"));
            SequenceFileOutputFormat.setOutputPath(sampleJob,outputStage);

            int code = sampleJob.waitForCompletion(true) ? 0 : 1;

            if(code == 0) {
                Job orderJob = Job.getInstance(configuration,"TotalOrderSortingStage");
                orderJob.setMapperClass(Mapper.class);
                orderJob.setReducerClass(ValueReducer.class);
                orderJob.setNumReduceTasks(10);
                orderJob.setPartitionerClass(TotalOrderPartitioner.class);
                TotalOrderPartitioner.setPartitionFile(configuration,partitionFile);
                orderJob.setOutputKeyClass(Text.class);
                orderJob.setOutputValueClass(Text.class);
                orderJob.setInputFormatClass(SequenceFileInputFormat.class);
                SequenceFileInputFormat.setInputPaths(orderJob,outputStage);
                FileUtil.fullyDelete(new File(args[1]));
                TextOutputFormat.setOutputPath(orderJob,outputOrder);
                orderJob.getConfiguration().set("mapred.textoutputformat.separator","");
                InputSampler.writePartitionFile(orderJob,new InputSampler.RandomSampler(1,20));
                code = orderJob.waitForCompletion(true) ? 0 : 2;
            }
            FileSystem.get(new Configuration()).delete(partitionFile,false);
            FileSystem.get(new Configuration()).delete(outputStage,true);
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
