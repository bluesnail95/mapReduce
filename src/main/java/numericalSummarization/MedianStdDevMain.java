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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;

/**
 * @Author bluesnail95
 * @Date 2019/7/16 6:18
 * @Description
 */
public class MedianStdDevMain {

    private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    public static class MedianStdDevMapper extends Mapper<Object, Text,IntWritable, IntWritable> {

        private IntWritable outhour = new IntWritable();
        private IntWritable outlength = new IntWritable();

        public void map(Object key,Text value,Context context) {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.setDateFormat(frmt);
            try {
                CountAverageData countAverageData = objectMapper.readValue(value.toString(), CountAverageData.class);
                Date creationDate = countAverageData.getCreationDate();
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(creationDate);
                int hour = calendar.get(Calendar.HOUR_OF_DAY);
                int length = countAverageData.getText().length();
                outhour.set(hour);
                outlength.set(length);
                context.write(outhour, outlength);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class MadianStdDevReducer extends Reducer<IntWritable, IntWritable, IntWritable, MedianStdDevTuple> {

        private ArrayList<Float> lengths = new ArrayList<Float>();
        private MedianStdDevTuple medianStdDevTuple = new MedianStdDevTuple();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) {
            int sum = 0;
            int count = 0;
            try {
                for (IntWritable value : values) {
                    sum += value.get();
                    count++;
                    lengths.add((float) value.get());
                }

                //进行排序
                Collections.sort(lengths);
                //求中位数
                if(count == 1 || count % 2 == 0) {
                    medianStdDevTuple.setMedian(lengths.get(count/2));
                }else {
                    medianStdDevTuple.setMedian((lengths.get(count / 2 - 1) + lengths.get(count / 2)) / 2.0f);
                }
                //求平均值
                float mean = sum / count;
                float sumOfSquare = 0.0f;
                //求标准差
                for(Float value: lengths) {
                    sumOfSquare += (value - mean) * (value - mean);
                }
                if(count == 1) {
                    medianStdDevTuple.setStdDev(0);
                }else{
                    medianStdDevTuple.setStdDev((float)Math.sqrt(sumOfSquare / (count - 1)));
                }

                context.write(key, medianStdDevTuple);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration, "CountAverage");
            job.setJarByClass(MedianStdDevMain.class);
            job.setMapperClass(MedianStdDevMapper.class);
            job.setReducerClass(MadianStdDevReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileUtil.deleteFile(args[1]);
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true )? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
