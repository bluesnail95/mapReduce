package jobmerge;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Random;

/**
 * @Author bluesnail95
 * @Date 2019/8/4 8:20
 * @Description
 */
public class JobMergeMain {

    public static class TaggedText implements WritableComparable<TaggedText> {

        private String tag = "";
        private Text text = new Text();

        public TaggedText() {}

        public String getTag() {
            return tag;
        }

        public void setTag(String tag) {
            this.tag = tag;
        }

        public Text getText() {
            return text;
        }

        public void setText(Text text) {
            this.text = text;
        }

        public int compareTo(TaggedText o) {
            int compare = this.tag.compareTo(o.getTag());
            if(compare == 0) {
                return this.text.compareTo(o.getText());
            }
            return compare;
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(tag);
            text.write(dataOutput);
        }

        public void readFields(DataInput dataInput) throws IOException {
            tag = dataInput.readUTF();
            text.readFields(dataInput);
        }

        @Override
        public String toString() {
            return "{" +
                    "tag='" + tag + '\'' +
                    ", text=" + text +
                    '}';
        }
    }

    public static class AnonymizeDistinctMergedMapper extends Mapper<Object,Text,TaggedText,Text> {

        private static final Text DISTINCT_OUT_VALUE = new Text();
        private Random random = new Random();
        private TaggedText anonymizeOutKey = new TaggedText();
        private TaggedText distinctOutKey = new TaggedText();
        private Text anonymizeOutValue = new Text();

        public void map(Object key,Text value,Context context) {
            anonymizeMapper(key,value,context);
            distinctMapper(key,value,context);
        }

        //匿名化
        private void anonymizeMapper(Object key, Text value, Context context) {
            JSONObject valueJson = JSONObject.parseObject(value.toString());
            valueJson.remove("userId");
            anonymizeOutKey.setTag("A");
            anonymizeOutKey.setText(new Text(String.valueOf(random.nextInt())));
            anonymizeOutValue.set(valueJson.toString());
            try {
                context.write(anonymizeOutKey,anonymizeOutValue);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        private void distinctMapper(Object key, Text value, Context context) {
            JSONObject valueJson = JSONObject.parseObject(value.toString());
            String userId = valueJson.getString("userId");
            distinctOutKey.setText(new Text(userId));
            distinctOutKey.setTag("D");
            try {
                context.write(distinctOutKey,DISTINCT_OUT_VALUE);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static class AnonymizeDistinctMergedReducer extends Reducer<TaggedText,Text,Text, NullWritable> {

        private MultipleOutputs<Text,NullWritable> mos = null;

        protected void setup(Context context) {
            mos = new MultipleOutputs<Text, NullWritable>(context);
        }

        public void reduce(TaggedText key,Iterable<Text> values,Context context) {
            if(key.getTag().equals("A")) {
                anonymizeReducer(key.getText(),values,context);
            }else {
                distinctReducer(key.getText(),values,context);
            }
        }

        private void distinctReducer(Text text, Iterable<Text> values, Context context) {
            try {
                mos.write("distinct",text,NullWritable.get(),"distinct/part");
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        private void anonymizeReducer(Text text, Iterable<Text> values, Context context) {
            for(Text value:values) {
                try {
                    mos.write("anonymize",value,NullWritable.get(),"anonymize/part");
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        public void cleanup(Context context) {
            try {
                mos.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        Job mergeJob = null;
        try {
            mergeJob = Job.getInstance(new Configuration(),"job merge");
        } catch (IOException e) {
            e.printStackTrace();
        }
        mergeJob.setJarByClass(JobMergeMain.class);
        mergeJob.setMapperClass(AnonymizeDistinctMergedMapper.class);
        mergeJob.setReducerClass(AnonymizeDistinctMergedReducer.class);
        mergeJob.setNumReduceTasks(1);
        try {
            TextInputFormat.addInputPath(mergeJob,new Path(args[0]));
            FileUtil.fullyDelete(new File(args[1]));
            TextOutputFormat.setOutputPath(mergeJob,new Path(args[1]));
        } catch (IOException e) {
            e.printStackTrace();
        }

        MultipleOutputs.addNamedOutput(mergeJob,"distinct",TextOutputFormat.class,Text.class,NullWritable.class);
        MultipleOutputs.addNamedOutput(mergeJob,"anonymize",TextOutputFormat.class,Text.class,NullWritable.class);

        mergeJob.setOutputKeyClass(TaggedText.class);
        mergeJob.setOutputValueClass(Text.class);

        try {
            System.exit(mergeJob.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
