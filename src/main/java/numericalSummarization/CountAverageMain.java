package numericalSummarization;

import file.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @Author bluesnail95
 * @Date 2019/7/14 21:40
 * @Description
 */
public class CountAverageMain {

    public static class CountAverageMapper extends Mapper<Object, Text, IntWritable, CountAverageTuple> {

        private IntWritable outHour = new IntWritable();
        private CountAverageTuple countAverageTuple = new CountAverageTuple();
        private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

        public void map(Object key,Text value,Context context) {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.setDateFormat(frmt);
            try {
                CountAverageData countAverageData = objectMapper.readValue(value.toString(), CountAverageData.class);
                Calendar calendar = Calendar.getInstance();
                Date creationDate = countAverageData.getCreationDate();
                calendar.setTime(creationDate);
                int hour = calendar.get(Calendar.HOUR_OF_DAY);
                outHour.set(hour);
                countAverageTuple.setAverage(countAverageData.getText().length());
                countAverageTuple.setCount(1);
                context.write(outHour, countAverageTuple);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class CountAverageReducer extends Reducer<IntWritable, CountAverageTuple,IntWritable, CountAverageTuple> {

        private CountAverageTuple result = new CountAverageTuple();

        public void reduce(IntWritable key, Iterable<CountAverageTuple> values,Context context) {
            float sum = 0;
            long count = 0;
            for(CountAverageTuple countAverageTuple : values) {
                count += countAverageTuple.getCount();
                sum += countAverageTuple.getCount() * countAverageTuple.getAverage();
            }
            result.setAverage(sum / count);
            result.setCount(count);
            try {
                context.write(key, result);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) {

        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration, "CountAverage");
            job.setJarByClass(CountAverageMain.class);
            job.setMapperClass(CountAverageMapper.class);
            job.setCombinerClass(CountAverageReducer.class);
            job.setReducerClass(CountAverageReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(CountAverageTuple.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileUtil.deleteFile(args[1]);
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true )? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
