package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @Author liuffei
 * @Date 2019/7/13 9:41
 * @Description
 */
public class CommentWordCount {

    //Mapper<Object, Text,Text, IntWritable>表示输入键，输入值，输出键，输出值
    //mapper输入的键值是在作业配置的FileInputFormat中定义的。
    public static class WordCountMapper extends Mapper<Object, Text,Text, IntWritable> {
        IntWritable one = new IntWritable(1);
        Text word = new Text();

        //覆盖了Mapper类的map方法
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String txt = value.toString();
            //将输入值中的非字母替换为空字符串
            txt = txt.replaceAll("[^a-zA-Z]","");
            StringTokenizer stringTokenizer = new StringTokenizer(txt);
            while(stringTokenizer.hasMoreTokens()) {
                word.set(stringTokenizer.nextToken());
                //将每个单词计数为1，并保存。
                context.write(word, one);
            }
        }
    }

    //Reducer<Text, IntWritable,Text, IntWritable>表示输入键，输入值，输出键，输出值
    //Reducer的输入键输入值应该和Mapper的输出键输出值得类型保持一致
    public static class IntSumReducer extends Reducer<Text, IntWritable,Text, IntWritable> {

        public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
            int sum = 0;
            for (IntWritable val:values) {
                sum += val.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

    public static void main(String[] args){
        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if(otherArgs.length != 2) {
                System.err.println("need enter input and output directory path");
                System.exit(2);
            }
            Job job = Job.getInstance(conf, "Word Count");
            //与自己定义的类名保持一致
            job.setJarByClass(CommentWordCount.class);
            //与自己定义的Mapper类和Reducer类保持一致
            job.setMapperClass(WordCountMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            //设置的输出键和输出值和mapper定义的需要保持一致。
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            //输入输出路径
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileUtil.fullyDelete(new File(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
            System.exit(job.waitForCompletion(true)?0:1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}