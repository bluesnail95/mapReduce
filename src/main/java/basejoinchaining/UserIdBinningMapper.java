package basejoinchaining;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;

/**
 * @Author bluesnail95
 * @Date 2019/7/29 23:21
 * @Description
 */
public class UserIdBinningMapper extends Mapper<Object,Text,Text,Text> {

    public static final String AVERAGE_POSTS_PER_USER = "avg.posts.per.use";

    public static double getAveragePostsPerUser(Configuration conf) {
        return Double.parseDouble(conf.get(AVERAGE_POSTS_PER_USER));
    }

    public static void setAveragePostsPerUser(Job job, double args) {
        job.getConfiguration().set(AVERAGE_POSTS_PER_USER,Double.toString(args));
    }

    private double average = 0.0;
    private MultipleOutputs<Text,Text> mos = null;
    private  Text outKey = new Text(), outValue = new Text();
    private HashMap<String,String> userIdToReputation = new HashMap<String,String>();

    protected void setup(Context context) {
        try {
            average = getAveragePostsPerUser(context.getConfiguration());
            mos = new MultipleOutputs<Text, Text>(context);
            URI[] uris = context.getCacheFiles();
            URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
            for(URI uri:uris) {
                URL url = uri.toURL();
                URLConnection connection = url.openConnection();
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                String line;
                while((line = bufferedReader.readLine()) != null) {
                    JSONObject lineJson = JSONObject.parseObject(line);
                    String userId = lineJson.getString("userId");
                    String reputation = lineJson.getString("reputation");
                    userIdToReputation.put(userId,reputation);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void map(Object key,Text value,Context context) {
        String[] tokens = value.toString().split("\t");
        String userId = tokens[0];
        int posts = Integer.parseInt(tokens[1]);
        outKey.set(userId);
        outValue.set(posts + "\t" + userIdToReputation.get(userId));
        try {
//            if(posts < average) {
//                mos.write("below",outKey,outValue,"below" + "/part");
//            }else{
//                mos.write("above",outKey,outValue,"above" + "/part");
//            }
            context.write(outKey,outValue);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void cleanup () throws IOException, InterruptedException {
        mos.close();
    }
}
