package hierarchical;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import filtering.GrepMain;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * @Author bluesnail95
 * @Date 2019/7/21 8:50
 * @Description
 */
public class PostCommentMain {

    public static class PostMapper extends Mapper<Object, Text,Text,Text> {

        public void map(Object key,Text value,Context context) {
            JSONObject textJson = JSONObject.parseObject(value.toString());
            String id = textJson.getString("id");
            if(StringUtils.isNoneBlank(id)) {
                try {
                    textJson.put("type","P");
                    context.write(new Text(id),new Text(textJson.toString()));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static class CommentMapper extends Mapper<Object, Text,Text,Text> {

        public void map(Object key,Text value,Context context) {
            JSONObject textJson = JSONObject.parseObject(value.toString());
            String id = textJson.getString("postId");
            if(StringUtils.isNoneBlank(id)) {
                try {
                    textJson.put("type","C");
                    context.write(new Text(id),new Text(textJson.toString()));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static class PostCommentReducer extends Reducer<Text,Text,Text, NullWritable> {

        private JSONObject postJson = null;

        private JSONArray commentArray = new JSONArray();

        public void reduce(Text key,Iterable<Text> values,Context context) {
            postJson = null;
            commentArray.clear();
            for (Text value : values) {
                JSONObject valueJson = JSONObject.parseObject(value.toString());
                String type = valueJson.getString("type");
                if("P".equals(type)) {
                    postJson = valueJson;
                }else if("C".equals(type)) {
                    commentArray.add(valueJson);
                }
            }
            postJson.put("comemnt",commentArray);
            try {
                context.write(new Text(postJson.toString()),NullWritable.get());
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
            Job job = Job.getInstance(conf, "PostComment");
            //与自己定义的类名保持一致
            job.setJarByClass(PostCommentMain.class);
            //设置的输出键和输出值和mapper定义的需要保持一致。
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            //输入输出路径
            //与自己定义的Mapper类和Reducer类保持一致
            job.setReducerClass(PostCommentReducer.class);
            MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class,PostMapper.class);
            MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class,CommentMapper.class);
            FileUtil.fullyDelete(new File(args[2]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true)?0:1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
