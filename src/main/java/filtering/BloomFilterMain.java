package filtering;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.io.File;
import java.net.URI;

/**
 * @Author bluesnail95
 * @Date 2019/7/20 15:35
 * @Description
 */
public class BloomFilterMain {

    public static class BloomFilterMapper extends Mapper<Object, Text,Text, NullWritable> {
        int vectorSize = BloomFilterUtil.getOptimalBloomFilterSize(10,0.1f);
        int nbHash = BloomFilterUtil.getOptimalK(10,vectorSize);
        BloomFilter bloomFilter = new BloomFilter(vectorSize, nbHash, Hash.MURMUR_HASH);

        protected void setup(Context context) {
            try {
                bloomFilter.add(new Key("BeiJing".getBytes()));
                bloomFilter.add(new Key("ShangHai".getBytes()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void map(Object key,Text value,Context context) {
            String word = value.toString();
            if(bloomFilter.membershipTest(new Key(word.getBytes()))) {
                try {
                    context.write(value,NullWritable.get());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Grep");
//            job.addCacheFile(new URI(args[2]));
            //与自己定义的类名保持一致
            job.setJarByClass(BloomFilterMain.class);
            //与自己定义的Mapper类和Reducer类保持一致
            job.setMapperClass(BloomFilterMapper.class);
            //设置的输出键和输出值和mapper定义的需要保持一致。
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
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
