package invertedIndex;

import file.FileUtil;
import numericalSummarization.MedianStdDevMain;
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
import java.util.Iterator;

/**
 * @Author bluesnail95
 * @Date 2019/7/18 23:18
 * @Description
 */
public class ExtractorMain  {

    public static class ExtractorMapper extends Mapper<Object, Text,Text,Text> {

        private Text link = new Text();
        private Text id = new Text();

        public void map(Object key,Text value,Context context) {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                ExtractorData extractorData = objectMapper.readValue(value.toString(), ExtractorData.class);
                link.set(extractorData.getLink());
                id.set(extractorData.getId());
                context.write(link,id);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    public static class ExtractorReducer extends Reducer<Text,Text,Text,Text> {
        private Text link = new Text();
        private Text ids = new Text();

        public void reduce(Text key, Iterable<Text> values,Context context) {
            StringBuilder buffer = new StringBuilder("");
            for(Text value:values) {
                buffer.append(value.toString());
                buffer.append(",");
            }
            ids.set(buffer.toString().substring(0, buffer.length() - 1));
            link.set(key.toString());
            try {
                context.write(link,ids);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration, "ExtractorMain");
            job.setJarByClass(ExtractorMain.class);
            job.setMapperClass(ExtractorMapper.class);
            job.setCombinerClass(ExtractorReducer.class);
            job.setReducerClass(ExtractorReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileUtil.deleteFile(args[1]);
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true )? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
