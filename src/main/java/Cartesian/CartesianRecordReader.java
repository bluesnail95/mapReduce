package Cartesian;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.join.CompositeInputSplit;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * @Author bluesnail95
 * @Date 2019/7/28 8:50
 * @Description
 */
public class CartesianRecordReader<K1,V1,K2,V2> implements RecordReader<Text,Text> {

    private RecordReader leftRecordReader = null,rightRecordReader = null;
    private FileInputFormat rightInputFormat;
    private JobConf rightConf;
    private InputSplit rightInputSplit;
    private Reporter rightReporter;

    private K1 lkey;
    private V1 lvalue;
    private K2 rkey;
    private V2 rvalue;
    private boolean goToNextLeft = true,alldone = false;

    public CartesianRecordReader(CompositeInputSplit split,JobConf conf,Reporter reporter) throws IOException {
        this.rightConf = conf;
        this.rightInputSplit = split.get(1);
        this.rightReporter = reporter;

        FileInputFormat leftFileInputFormat = null;
        try {
            leftFileInputFormat = (FileInputFormat) ReflectionUtils.newInstance(Class.forName(conf.get(CartesianInputFormat.LEFT_INPUT_FORMAT)),conf);
            leftRecordReader = leftFileInputFormat.getRecordReader(split.get(0), conf, reporter);
            rightInputFormat = (FileInputFormat) ReflectionUtils.newInstance(Class.forName(conf.get(CartesianInputFormat.RIGHT_INPUT_FORMAT)),conf);
            rightRecordReader = rightInputFormat.getRecordReader(rightInputSplit,rightConf,rightReporter);

            lkey = (K1) this.leftRecordReader.createKey();
            lvalue = (V1) this.leftRecordReader.createValue();

            rkey = (K2) this.rightRecordReader.createKey();
            rvalue = (V2) this.rightRecordReader.createValue();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    public boolean next(Text key, Text value) throws IOException {
        if(null == key || null == value) {
            alldone = true;
            return alldone;
        }
        try {
            do {
                if(goToNextLeft) {
                    if(!leftRecordReader.next(lkey,lvalue)) {
                        alldone = true;
                        break;
                    }else{
                        key.set(lvalue.toString());
                        goToNextLeft = alldone = false;
                        this.rightRecordReader = this.rightInputFormat.getRecordReader(this.rightInputSplit,this.rightConf,this.rightReporter);
                    }
                }
                if(rightRecordReader.next(rkey,rvalue)) {
                    value.set(rvalue.toString());
                }else{
                    goToNextLeft = true;
                }
            }while (goToNextLeft);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return !alldone;
    }

    public Text createKey() {
        return null;
    }

    public Text createValue() {
        return null;
    }

    public long getPos() throws IOException {
        return 0;
    }

    public void close() throws IOException {

    }

    public float getProgress() throws IOException {
        return 0;
    }
}
