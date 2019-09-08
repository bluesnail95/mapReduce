package numericalSummarization;

import file.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Author bluesnail95
 * @Date 2019/7/16 21:28
 * @Description
 */
public class MedianStdDevUpgradeMain {

    private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    public static class MedianStdDevUpgradeMapper extends Mapper<Object, Text, IntWritable, SortedMapWritable> {
        private IntWritable outHour = new IntWritable();
        private LongWritable one = new LongWritable(1);
        private IntWritable lengths = new IntWritable();

        public void map(Object key,Text value,Context context) {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.setDateFormat(frmt);
            try {
                CountAverageData countAverageData = objectMapper.readValue(value.toString(), CountAverageData.class);
                Date creationDate = countAverageData.getCreationDate();
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(creationDate);
                outHour.set(calendar.get(Calendar.HOUR_OF_DAY));
                lengths.set(countAverageData.getText().length());
                SortedMapWritable sortedMapWritable = new SortedMapWritable();
                sortedMapWritable.put(lengths,one);
                context.write(outHour, sortedMapWritable);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class MedianStdDevUpgradeCombiner extends Reducer<IntWritable,SortedMapWritable,IntWritable,SortedMapWritable> {

        protected void reduce(IntWritable key,Iterable<SortedMapWritable> values,Context context) {
            SortedMapWritable outValue = new SortedMapWritable();

            try {
                for (SortedMapWritable sortedMapWritable : values) {
                    Set<Map.Entry<WritableComparable,Writable>> set = sortedMapWritable.entrySet();
                    Iterator<Map.Entry<WritableComparable,Writable>> iterator = set.iterator();
                    while(iterator.hasNext()) {
                        Map.Entry<WritableComparable,Writable> entry = iterator.next();

                       LongWritable count = (LongWritable) outValue.get(entry.getKey());
                       if(count != null) {
                            count.set(count.get() + ((LongWritable)entry.getValue()).get());
                            outValue.put(entry.getKey(), count);
                       }else{
                           outValue.put(entry.getKey(),new LongWritable(((LongWritable)entry.getValue()).get()));
                       }

                    }
                }
                context.write(key, outValue);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class MedianStdDevUpgradeReducer extends Reducer<IntWritable,SortedMapWritable,IntWritable,MedianStdDevTuple> {
        private MedianStdDevTuple medianStdDevTuple = new MedianStdDevTuple();
        private TreeMap<Integer, Long> lengthCounts = new TreeMap<Integer, Long>();

        public void reduce(IntWritable key,Iterable<SortedMapWritable> values,Context context) {
            float sum = 0;
            long total = 0;
           lengthCounts.clear();
            medianStdDevTuple.setStdDev(0);
            medianStdDevTuple.setMedian(0);

            for(SortedMapWritable sortedMapWritable : values) {
                Set<Map.Entry<WritableComparable,Writable>> set = sortedMapWritable.entrySet();
                Iterator<Map.Entry<WritableComparable,Writable>> iterator =  set.iterator();
               while (iterator.hasNext()) {
                   Map.Entry<WritableComparable,Writable> writableEntry = iterator.next();
                   int length = ((IntWritable)writableEntry.getKey()).get();
                   long count = ((LongWritable)writableEntry.getValue()).get();

                   total += count;
                   sum += count * length;
                   Long sortedCount = lengthCounts.get(length);
                   if(sortedCount == null) {
                       lengthCounts.put(length, count);
                   }else{
                       lengthCounts.put(length, count + sortedCount);
                   }
               }
            }

            long medianIndex = total / 2;
            long previousCount = 0;
            long count = 0;
            long prevKey = 0;
            for(Map.Entry<Integer, Long> entry:lengthCounts.entrySet()) {
                count = previousCount + entry.getValue();
                if(previousCount <= medianIndex && medianIndex < count) {
                    if(total % 2 == 0 && previousCount == medianIndex) {
                        medianStdDevTuple.setMedian((entry.getKey() + prevKey) / 2.0f);
                    }else{
                        medianStdDevTuple.setMedian(entry.getKey());
                    }
                    break;
                }
                previousCount = count;
                prevKey = entry.getKey();
            }

            float mean = sum / total;
            float sumOfSquares = 0.0f;
            for(Map.Entry<Integer, Long> entry:lengthCounts.entrySet()) {
                sumOfSquares += (entry.getKey() - mean) * (entry.getKey() - mean) * entry.getValue();
            }
            if(total == 1) {
                medianStdDevTuple.setStdDev(0);
            }else{
                medianStdDevTuple.setStdDev((float)Math.sqrt((sumOfSquares / (total - 1))));
            }
            try {
                context.write(key, medianStdDevTuple);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration, "MedianStdDevUpgrade");
            job.setJarByClass(MedianStdDevUpgradeMain.class);
            job.setMapperClass(MedianStdDevUpgradeMapper.class);
            job.setCombinerClass(MedianStdDevUpgradeCombiner.class);
            job.setReducerClass(MedianStdDevUpgradeReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(SortedMapWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileUtil.deleteFile(args[1]);
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true )? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
