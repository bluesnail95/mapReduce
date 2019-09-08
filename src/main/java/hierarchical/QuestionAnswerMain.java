package hierarchical;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * @Author bluesnail95
 * @Date 2019/7/21 10:01
 * @Description
 */
public class QuestionAnswerMain {

    public static class QuestionAnswerMapper extends Mapper<Object, Text,Text, Text> {

        public void map(Object key,Text value,Context context) {
            JSONObject valueJson = JSONObject.parseObject(value.toString());
            String parentId = valueJson.getString("parentId");
            try {
                if(StringUtils.isNotBlank(parentId)) {
                    context.write(new Text(parentId),value);
                }else {
                    String id = valueJson.getString("id");
                    context.write(new Text(id),value);
                }
            }catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class QuestionAnswerReducer extends Reducer<Text,Text,Text,NullWritable> {

        private JSONObject questionJson = null;

        private JSONArray answerArray = new JSONArray();

        public void reduce(Text key, Iterable<Text> values, Context context) {
            questionJson = null;
            answerArray.clear();
            for (Text value:values) {
                JSONObject valueJson = JSONObject.parseObject(value.toString());
                String parentId = valueJson.getString("parentId");
                if(StringUtils.isNotBlank(parentId)) {
                    answerArray.add(valueJson);
                }else{
                    questionJson = valueJson;
                }
            }
            questionJson.put("answer",answerArray);
            try {
                context.write(new Text(questionJson.toString()),NullWritable.get());
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "question and answer");
            //与自己定义的类名保持一致
            job.setJarByClass(QuestionAnswerMain.class);
            //与自己定义的Mapper类和Reducer类保持一致
            job.setMapperClass(QuestionAnswerMapper.class);
            job.setReducerClass(QuestionAnswerReducer.class);
            //设置的输出键和输出值和mapper定义的需要保持一致。
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            //输入输出路径
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileUtil.fullyDelete(new File(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true)?0:1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
