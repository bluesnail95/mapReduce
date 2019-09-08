package Cartesian;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.join.CompositeInputSplit;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * @Author bluesnail95
 * @Date 2019/7/28 8:32
 * @Description
 */
public class CartesianInputFormat extends FileInputFormat {

    public static final String LEFT_INPUT_FORMAT = "cart.left.inputformat";
    public static final String LEFT_INPUT_PATH = "cart.left.path";
    public static final String RIGHT_INPUT_FORMAT = "cart.right.inputformat";
    public static final String RIGHT_INPUT_PATH = "cart.right.path";
    private JobConf job;
    private int numSplits;

    public static void setLeftInputInfo(JobConf job, Class<? extends FileInputFormat> inputFormat,String inputPath) {
        job.set(LEFT_INPUT_FORMAT,inputFormat.getCanonicalName());
        job.set(LEFT_INPUT_PATH,inputPath);
    }

    public static void setRightInputInfo(JobConf job,Class<? extends  FileInputFormat> inputFormat,String inputPath) {
        job.set(RIGHT_INPUT_FORMAT,inputFormat.getCanonicalName());
        job.set(RIGHT_INPUT_PATH,inputPath);
    }

    public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        return new CartesianRecordReader((CompositeInputSplit)inputSplit,jobConf,reporter);
    }

    private InputSplit[] getInputSplits(JobConf conf,String inputFormatClass,String inputPath,int numSplits) {
        try {
            FileInputFormat fileInputFormat = (FileInputFormat) ReflectionUtils.newInstance(Class.forName(inputFormatClass),conf);
            fileInputFormat.setInputPaths(conf,inputPath);
            return fileInputFormat.getSplits(conf,numSplits);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        CompositeInputSplit returnSplits[] = new CompositeInputSplit[0];
        try {
            InputSplit[] leftSplits = getInputSplits(job, job.get(LEFT_INPUT_FORMAT), job.get(LEFT_INPUT_PATH), numSplits);
            InputSplit[] rightSplits = getInputSplits(job, job.get(RIGHT_INPUT_FORMAT), job.get(RIGHT_INPUT_PATH), numSplits);
            returnSplits = new CompositeInputSplit[leftSplits.length * rightSplits.length];
            int i = 0;
            for(InputSplit left:leftSplits) {
                for(InputSplit right:rightSplits) {
                    returnSplits[i] = new CompositeInputSplit(2);
                    returnSplits[i].add(left);
                    returnSplits[i].add(right);
                    ++i;
                }
            }
            LOG.info("Total splits to process: " + returnSplits.length);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return returnSplits;
    }
}
