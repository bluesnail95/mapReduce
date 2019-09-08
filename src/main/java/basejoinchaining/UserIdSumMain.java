package basejoinchaining;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

import java.io.File;

/**
 * @Author bluesnail95
 * @Date 2019/7/30 6:40
 * @Description
 */
public class UserIdSumMain {
    public static final String AVERAGE_CALC_GROUP = "Average";

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        Path userInput = new Path(args[0]);
        Path postInput = new Path(args[1]);
        Path outputDirIntermediate = new Path(args[2] + "_int");
        Path outputDir = new Path(args[2]);
        try {
            Job countingJob = Job.getInstance(conf,"userIdSumMain");
            countingJob.setJarByClass(UserIdSumMain.class);
            countingJob.setMapperClass(UserIdCountMapper.class);
            countingJob.setCombinerClass(LongSumReducer.class);
            countingJob.setReducerClass(UserIdSumReducer.class);
            countingJob.setOutputKeyClass(Text.class);
            countingJob.setOutputValueClass(LongWritable.class);
            countingJob.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.addInputPath(countingJob,postInput);
            countingJob.setOutputFormatClass(TextOutputFormat.class);
            FileUtil.fullyDelete(new File(args[2] + "_int"));
            TextOutputFormat.setOutputPath(countingJob,outputDirIntermediate);
            int code = countingJob.waitForCompletion(true) ? 0 : 1;
            if(code == 0) {
                double numRecords = countingJob.getCounters().findCounter(AVERAGE_CALC_GROUP,UserIdCountMapper.RECORDS_COUNTER_NAME).getValue();
                double numUsers = countingJob.getCounters().findCounter(AVERAGE_CALC_GROUP,UserIdSumReducer.USERS_COUNTER_NAME).getValue();

                double averagePostPerUser = numRecords / numUsers;

                Job binningJob = Job.getInstance(new Configuration(),"JobChaining-Bining");
                binningJob.setJarByClass(UserIdSumMain.class);
                binningJob.setMapperClass(UserIdBinningMapper.class);
                UserIdBinningMapper.setAveragePostsPerUser(binningJob,averagePostPerUser);
                binningJob.setNumReduceTasks(0);
                binningJob.setInputFormatClass(TextInputFormat.class);
                TextInputFormat.addInputPath(binningJob,outputDirIntermediate);

//                MultipleOutputs.addNamedOutput(binningJob,"below",TextOutputFormat.class,Text.class,Text.class);
//                MultipleOutputs.addNamedOutput(binningJob,"above",TextOutputFormat.class,Text.class,Text.class);
//                MultipleOutputs.setCountersEnabled(binningJob,true);
                FileUtil.fullyDelete(new File(args[2]));
                TextOutputFormat.setOutputPath(binningJob,outputDir);
                FileStatus[] userFiles = FileSystem.get(conf).listStatus(userInput);
                for(FileStatus status:userFiles) {
                    binningJob.addCacheFile(status.getPath().toUri());
                }
                code = binningJob.waitForCompletion(true) ? 0 : 1;
            }
            FileSystem.get(conf).delete(outputDirIntermediate,true);
            System.exit(code);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
