package wordcount;

import file.FileUtil;
import invertedIndex.ExtractorMain;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Arrays;
import java.util.HashSet;

/**
 * @Author bluesnail95
 * @Date 2019/7/19 6:31
 * @Description
 */
public class CountNumUsersByStateMain {

    public static class CountNumUsersByStateMapper extends Mapper<Object, Text, NullWritable,NullWritable> {
        public static final String STATE_COUNTER_GROUP = "State";
        public static final String UNKNOWN_COUNTER = "Unknown";
        public static final String NULL_OR_EMPTY_COUNTER = "Null or Empty";

        private String stateArray[] = {"BeiJing","ShangHai","ShenZhen","GuangZhou"};

        private HashSet<String> stateSet = new HashSet<String>(Arrays.asList(stateArray));

        public void map(Object key,Text value,Context context) {
            String state = value.toString();
            if(state != null && StringUtils.isNoneBlank(state)) {
                if(stateSet.contains(state)) {
                    context.getCounter(STATE_COUNTER_GROUP,state).increment(1);
                }else{
                    context.getCounter(STATE_COUNTER_GROUP,UNKNOWN_COUNTER).increment(1);
                }
            }else {
                context.getCounter(STATE_COUNTER_GROUP,NULL_OR_EMPTY_COUNTER).increment(1);
            }
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration, "CountNumUsersByState");
            job.setJarByClass(CountNumUsersByStateMain.class);
            job.setMapperClass(CountNumUsersByStateMapper.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(NullWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileUtil.deleteFile(args[1]);
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            int code = job.waitForCompletion(true )? 0 : 1;
            if(code == 0) {
                for(Counter counter:job.getCounters().getGroup(CountNumUsersByStateMapper.STATE_COUNTER_GROUP)) {
                    System.out.println(counter.getDisplayName() + "\t" + counter.getValue());
                }
            }
            FileSystem.get(configuration).delete(new Path(args[1]),true);
            System.exit(code);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
