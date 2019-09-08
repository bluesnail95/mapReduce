package reduce;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.apache.hadoop.mapred.join.TupleWritable;

import java.io.File;
import java.io.IOException;

/**
 * @Author bluesnail95
 * @Date 2019/7/27 19:12
 * @Description 组合模式
 */
public class CompositeMain {

    public static class CompositeMapper extends MapReduceBase implements Mapper<Text, TupleWritable,Text, TupleWritable> {


        public void map(Text text, TupleWritable writables, OutputCollector<Text, TupleWritable> outputCollector, Reporter reporter) throws IOException {
            outputCollector.collect(text,writables);
        }
    }

    public static void main(String[] args) {
        Path userPath = new Path(args[0]);
        Path commentPath = new Path(args[1]);
        Path output = new Path(args[2]);
        try {
            JobConf jobconf = new JobConf();
            jobconf.setJarByClass(CompositeMain.class);
            jobconf.setMapperClass(CompositeMapper.class);
            jobconf.setNumReduceTasks(0);
            jobconf.setOutputValueClass(Text.class);
            jobconf.setOutputKeyClass(TupleWritable.class);
            jobconf.setInputFormat(CompositeInputFormat.class);
            jobconf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");
            jobconf.set("mapred.join.expr", CompositeInputFormat.compose("outer", KeyValueTextInputFormat.class,userPath,commentPath));
            FileUtil.fullyDelete(new File(args[2]));
            TextOutputFormat.setOutputPath(jobconf,output);
            RunningJob job = JobClient.runJob(jobconf);
            while(!job.isComplete()) {
                Thread.sleep(100);
            }
            System.exit(job.isSuccessful() ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
