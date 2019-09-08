package reduce;

import com.alibaba.fastjson.JSONObject;
import filtering.BloomFilterUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author bluesnail95
 * @Date 2019/7/27 9:20
 * @Description Reduce连接模式(内连接，外连接（左外连接，右外连接，全外连接），反连接
 */
public class ReduceMain {

    private static int vectorSize = BloomFilterUtil.getOptimalBloomFilterSize(10,0.1f);
    private static int nbHash = BloomFilterUtil.getOptimalK(10,vectorSize);
    private static BloomFilter bloomFilter = new BloomFilter(vectorSize, nbHash, Hash.MURMUR_HASH);

    public static class UserJoinMapper extends Mapper<Object,Text,Text,Text> {

        public void map(Object key, Text value, Context context) {
            JSONObject valueJson = JSONObject.parseObject(value.toString());
            String userId = valueJson.getString("userId");
            valueJson.put("type","U");
            try {
                Integer reputation = valueJson.getInteger("reputation");
                if(reputation > 2000) {
                    bloomFilter.add(new Key(userId.getBytes()));
                    context.write(new Text(userId),new Text(valueJson.toString()));
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    public static class CommentJoinMapper extends Mapper<Object,Text,Text,Text> {

        public void map(Object key, Text value, Context context) {
            JSONObject valueJson = JSONObject.parseObject(value.toString());
            String userId = valueJson.getString("userId");
            valueJson.put("type","C");
            try {
                if(bloomFilter.membershipTest(new Key(userId.getBytes()))) {
                    context.write(new Text(userId),new Text(valueJson.toString()));
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static class UserCommentReducer extends Reducer<Text,Text,Text,Text> {

        private Text empty = new Text(new JSONObject().toString());

        public void reduce(Text key,Iterable<Text> values,Context context) {
            List<Text> users = new ArrayList<Text>();
            List<Text> comments = new ArrayList<Text>();
            for (Text value:values) {
                JSONObject valueJson = JSONObject.parseObject(value.toString());
                String type = valueJson.getString("type");
                if(StringUtils.isNotBlank(type) && "U".equalsIgnoreCase(type)) {
                    users.add(value);
                }else if(StringUtils.isNotBlank(type) && "C".equalsIgnoreCase(type)) {
                    comments.add(value);
                }
            }

            //进行连接
            String joinType = context.getConfiguration().get("join.type");
            if("innerjoin".equalsIgnoreCase(joinType)) {
                innerJoin(users,comments,context);
            }else if("leftjoin".equalsIgnoreCase(joinType)) {
                leftJoin(users,comments,context);
            }else if("rightjoin".equalsIgnoreCase(joinType)) {
                rightJoin(users,comments,context);
            }else if("outjoin".equalsIgnoreCase(joinType)) {
                outJoin(users,comments,context);
            }else if("antijoin".equalsIgnoreCase(joinType)) {
                antiJoin(users,comments,context);
            }
        }

        //内连接
        public void innerJoin(List<Text> users,List<Text> comments,Context context) {
            if(null == users || users.size() == 0 || null == comments || comments.size() == 0) {
                return;
            }
            for (Text user:users) {
                for(Text comment:comments) {
                    try {
                        context.write(user,comment);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        public void leftJoin(List<Text> users,List<Text> comments,Context context) {
            if(null == users || users.size() == 0) {
                return;
            }
            for(Text user:users) {
                try {
                    if(null == comments || comments.size() == 0) {
                        context.write(user,empty);
                    }else{
                        for(Text comment:comments) {
                            context.write(user,comment);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        public void rightJoin(List<Text> users,List<Text> comments,Context context) {
            if(null == comments || comments.size() == 0) {
                return;
            }
            for(Text comment:comments) {
                try {
                    if(null == users || users.size() == 0) {
                        context.write(empty,comment);
                    }else{
                        for(Text user:users) {
                            context.write(user,comment);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        public void outJoin(List<Text> users,List<Text> comments,Context context) {
            try {
                if(null != users && users.size() > 0) {
                    for (Text user:users) {
                        if(null != comments && comments.size() > 0) {
                            for(Text comment:comments) {
                                context.write(user,comment);
                            }
                        }else{
                            context.write(user,empty);
                        }
                    }
                }else{
                    if(null != comments && comments.size() > 0) {
                        for(Text comment:comments) {
                            context.write(empty,comment);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void antiJoin(List<Text> users,List<Text> comments,Context context) {
            try {
                //^表示异或，当users或comments其中一个为空时，为true,当users和comments都为空或都不为空时取false。
                if(users.isEmpty() ^ comments.isEmpty()) {
                    if(users.isEmpty()) {
                        for (Text comment:comments) {
                            context.write(empty,comment);
                        }
                    }
                    if(comments.isEmpty()) {
                        for(Text user:users) {
                            context.write(user,empty);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("join.type",args[0]);
        try {
            Job job = Job.getInstance(conf,"reduce");
            MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class,UserJoinMapper.class);
            MultipleInputs.addInputPath(job,new Path(args[2]), TextInputFormat.class,CommentJoinMapper.class);
            job.setReducerClass(UserCommentReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileUtil.fullyDelete(new File(args[3]));
            FileOutputFormat.setOutputPath(job, new Path(args[3]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
