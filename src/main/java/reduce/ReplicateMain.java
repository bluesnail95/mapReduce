package reduce;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;

/**
 * @Author bluesnail95
 * @Date 2019/7/27 17:11
 * @Description 复制模式
 */
public class ReplicateMain {

    public static class ReplicateMapper extends Mapper<Object, Text,Text,Text> {

        private JSONObject userInfo = new JSONObject();
        private String joinType = "";
        private Text empty = new Text("");

        public void map(Object key,Text value,Context context) {
            JSONObject valueJson = JSONObject.parseObject(value.toString());
            String userId = valueJson.getString("userId");
            JSONObject joinInfo = userInfo.getJSONObject(userId);
            try {
                if(null != joinInfo) {
                    context.write(value,new Text(joinInfo.toString()));
                }else if("leftjoin".equalsIgnoreCase(joinType)) {
                    context.write(value,empty);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void setup(Context context) {
            try {
                URI uris[] = context.getCacheFiles();
                URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
                for(URI uri:uris) {
                    URL url = uri.toURL();
                    URLConnection connection = url.openConnection();
                    InputStream inputStream = connection.getInputStream();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                    String line =  null;
                    while((line = reader.readLine()) != null) {
                        JSONObject lineJson = JSONObject.parseObject(line);
                        String userId = lineJson.getString("userId");
                        userInfo.put(userId,line);
                    }
                }
                joinType = context.getConfiguration().get("join.type");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("join.type",args[0]);
        try {
            Job job = Job.getInstance(conf,"replicate join");
            URI file = new URI("hdfs://127.0.0.1:9000/hadoop/1.txt");
            URI[] files = new URI[1];
            files[0] = file;
            job.setCacheFiles(files);
            job.setMapperClass(ReplicateMapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job,new Path(args[1]));
            FileUtil.fullyDelete(new File(args[2]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
