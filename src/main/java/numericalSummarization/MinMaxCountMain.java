package numericalSummarization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.text.SimpleDateFormat;

/**
 * @Author bluesnail95
 * @Date 2019/7/14 10:02
 * @Description
 */
public class MinMaxCountMain {

    public static class MinMaxCountMapper extends Mapper<Object,Text,Text,MinMaxCountTuple> {

        private Text userId = new Text();
        private MinMaxCountTuple minMaxCountTuple = new MinMaxCountTuple();
        private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

        public void map(Object key,Text value,Context context){
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.setDateFormat(frmt);
                MinMaxCountData minMaxCountData = objectMapper.readValue(value.toString(), MinMaxCountData.class);
                minMaxCountTuple.setCount(1);
                minMaxCountTuple.setMin(minMaxCountData.getCreateDate());
                minMaxCountTuple.setMax(minMaxCountData.getCreateDate());
                userId.set(minMaxCountData.getUserId());
                context.write(userId, minMaxCountTuple);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class MinMaxCountReducer extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {
        private MinMaxCountTuple minMaxCountTuple = new MinMaxCountTuple();

        public void reduce(Text key,Iterable<MinMaxCountTuple> values,Context context) {
            try {
                long sum = 0;
                for (MinMaxCountTuple value : values) {
                    if(minMaxCountTuple.getMin() == null || value.getMin().compareTo(minMaxCountTuple.getMin()) < 0 ) {
                        minMaxCountTuple.setMin(value.getMin());
                    }
                    if(minMaxCountTuple.getMax() == null || value.getMax().compareTo(minMaxCountTuple.getMax()) > 0 ) {
                        minMaxCountTuple.setMax(value.getMax());
                    }
                    sum += value.getCount();
                }
                minMaxCountTuple.setCount(sum);
                context.write(key, minMaxCountTuple);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf, "NumericalSummarization：MinMaxCount");
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(MinMaxCountTuple.class);
            job.setJarByClass(MinMaxCountMain.class);
            job.setMapperClass(MinMaxCountMapper.class);
            job.setCombinerClass(MinMaxCountReducer.class);
            job.setReducerClass(MinMaxCountReducer.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            File outputFile = new File(args[1]);
            if(outputFile.exists()){
                outputFile.delete();
            }
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
