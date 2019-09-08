package Cartesian;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.File;
import java.io.IOException;

/**
 * @Author bluesnail95
 * @Date 2019/7/28 10:04
 * @Description
 */
public class CartesianMain {

    public static void main(String[] args) {
        JobConf conf = new JobConf();
        conf.setJarByClass(CartesianMain.class);
        conf.setMapperClass(CartesianMapper.class);
        conf.setNumReduceTasks(0);
        conf.setInputFormat(CartesianInputFormat.class);

        CartesianInputFormat.setLeftInputInfo(conf, TextInputFormat.class,args[0]);
        CartesianInputFormat.setRightInputInfo(conf, TextInputFormat.class,args[0]);
        FileUtil.fullyDelete(new File(args[1]));
        TextOutputFormat.setOutputPath(conf,new Path(args[1]));

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        try {
            RunningJob job = JobClient.runJob(conf);
            while(!job.isComplete()){
                Thread.sleep(100);
            }
            System.exit(job.isSuccessful() ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
