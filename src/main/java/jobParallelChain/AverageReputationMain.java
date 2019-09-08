package jobParallelChain;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * @Author bluesnail95
 * @Date 2019/8/3 7:16
 * @Description
 */
public class AverageReputationMain {

    public static class AverageReputationMapper extends Mapper<Object, Text, NullWritable, DoubleWritable> {

        public void map(Object key,Text value,Context context) {
            JSONObject valueJson = JSONObject.parseObject(value.toString());
            Double reputation = valueJson.getDouble("reputation");
            DoubleWritable doubleWritable = new DoubleWritable();
            doubleWritable.set(reputation);
            try {
                context.write(NullWritable.get(),doubleWritable);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class AverageReputationReducer extends Reducer<NullWritable,DoubleWritable,NullWritable,DoubleWritable> {

        public void reduce(NullWritable key,Iterable<DoubleWritable> values,Context context) {
            int count = 0;
            double sum = 0;
            double average = 0;
            for(DoubleWritable doubleWritable:values) {
                count++;
                sum += doubleWritable.get();
            }
            average = sum / count;
            DoubleWritable result = new DoubleWritable();
            result.set(average);
            try {
                context.write(NullWritable.get(),result);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static Job submitJob(Configuration configuration, String inputDir, String outputDir) {
        try {
            Job job = Job.getInstance(configuration,"Parallel Job");
            job.setJarByClass(AverageReputationMain.class);

            job.setMapperClass(AverageReputationMapper.class);
            job.setReducerClass(AverageReputationReducer.class);

            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(DoubleWritable.class);

            job.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.addInputPath(job,new Path(inputDir));

            job.setOutputFormatClass(TextOutputFormat.class);
            FileUtil.fullyDelete(new File(outputDir));
            TextOutputFormat.setOutputPath(job,new Path(outputDir));
            job.submit();
            return job;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job arvJob1 = submitJob(configuration, args[0], args[2]);
            Job arvJob2 = submitJob(configuration, args[1], args[3]);

            while(!arvJob1.isComplete() || !arvJob2.isComplete()) {
                Thread.sleep(5000);
            }

            if(arvJob1.isSuccessful()) {
                System.out.println("The arvJob1 is successful");
            }else{
                System.out.println("The arvJob1 is not successful");
            }

            if(arvJob2.isSuccessful()) {
                System.out.println("The arvJob2 is successful");
            }else{
                System.out.println("The arvJob2 is not successful");
            }

            System.exit(arvJob1.isSuccessful() && arvJob2.isSuccessful() ? 0 :1);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
