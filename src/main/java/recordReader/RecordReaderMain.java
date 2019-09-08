package recordReader;

import com.alibaba.fastjson.JSONObject;
import file.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @Author bluesnail95
 * @Date 2019/8/9 16:40
 * @Description
 */
public class RecordReaderMain {

    public static final String NUM_MAP_TASKS = "random.generator.map.tasks";
    public static final String NUM_RECORDS_PER_TASKS = "random.generator.num.records.per.map.task";
    public static final String RANDOM_WORD_LIST = "random.generator.num.word.file";

    public static class FakeInputSplit extends InputSplit implements Writable {

        public void write(DataOutput dataOutput) throws IOException {

        }

        public void readFields(DataInput dataInput) throws IOException {

        }

        public long getLength() throws IOException, InterruptedException {
            return 0;
        }

        public String[] getLocations() throws IOException, InterruptedException {
            return new String[0];
        }
    }

    public static class RandomStackOverflowInputFormat extends InputFormat<Text, NullWritable> {

        public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
            int numSplits = jobContext.getConfiguration().getInt(NUM_MAP_TASKS, -1);
            ArrayList<InputSplit> inputSplitArrayList = new ArrayList<InputSplit>();
            for (int i = 0; i < numSplits; i++) {
                inputSplitArrayList.add(new FakeInputSplit());
            }
            return inputSplitArrayList;
        }

        public RecordReader<Text, NullWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            RandomStackOverflowReader reader = new RandomStackOverflowReader();
            reader.initialize(inputSplit,taskAttemptContext);
            return reader;
        }

        public static void setNumMapTasks(Job job,int i) {
            job.getConfiguration().setInt(NUM_MAP_TASKS,i);
        }

        public static void setNumRecordPerTask(Job job,int i) {
            job.getConfiguration().setInt(NUM_RECORDS_PER_TASKS,i);
        }

        public static void setRandomWordList(Job job, Path file) {
            job.addCacheFile(file.toUri());
        }
    }

    public static class RandomStackOverflowReader extends RecordReader<Text, NullWritable> {

        private int numRecordsToCreate = 0;
        private int createRecords = 0;
        private Text key = new Text();
        private NullWritable value = NullWritable.get();
        private Random random = new Random();
        private ArrayList<String> randomWords = new ArrayList<String>();
        private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            this.numRecordsToCreate = taskAttemptContext.getConfiguration().getInt(NUM_RECORDS_PER_TASKS, -1);
            URI[] files = taskAttemptContext.getCacheFiles();
            BufferedReader reader = new BufferedReader(new FileReader(files[0].toString()));
            String line = null;
            while((line = reader.readLine()) != null) {
                randomWords.add(line);
            }
            reader.close();
        }

        public boolean nextKeyValue() throws IOException, InterruptedException {
            if(createRecords < numRecordsToCreate) {
                int score = Math.abs(random.nextInt()) % 15000;
                int rowId = Math.abs(random.nextInt()) % 1000000000;
                int postId = Math.abs(random.nextInt()) % 10000000;
                int userId = Math.abs(random.nextInt()) % 1000000;
                String createDate = sdf.format(Math.abs(random.nextLong()));

                String text = getRandomText();
                JSONObject randomRecord = new JSONObject();
                randomRecord.put("id",rowId);
                randomRecord.put("postId",postId);
                randomRecord.put("score",score);
                randomRecord.put("text",text);
                randomRecord.put("userId",userId);
                randomRecord.put("createDate",createDate);
                key.set(randomRecord.toString());
                ++createRecords;
                return true;
            }
            return false;
        }

        public String getRandomText() {
            StringBuilder stringBuilder = new StringBuilder();
            int numWords = Math.abs(random.nextInt()) % 30 + 1;
            for (int i = 0; i < numWords; i++) {
                stringBuilder.append(randomWords.get(Math.abs(random.nextInt()) % randomWords.size()) + " ");
            }
            return stringBuilder.toString();
        }

        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        public NullWritable getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        public float getProgress() throws IOException, InterruptedException {
            return (float)createRecords / numRecordsToCreate;
        }

        public void close() throws IOException {

        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        int numMapTasks = Integer.parseInt(args[0]);
        int numRecordPerTask = Integer.parseInt(args[1]);
        Path wordList = new Path(args[2]);
        Path outputDir = new Path(args[3]);
        try {
            Job job = Job.getInstance(configuration,"record reader");
            job.setJarByClass(RecordReaderMain.class);
            job.setNumReduceTasks(0);
            job.setInputFormatClass(RandomStackOverflowInputFormat.class);
            RandomStackOverflowInputFormat.setNumMapTasks(job,numMapTasks);
            RandomStackOverflowInputFormat.setNumRecordPerTask(job,numRecordPerTask);
            RandomStackOverflowInputFormat.setRandomWordList(job,wordList);
            FileUtil.deleteFile(args[3]);
            TextOutputFormat.setOutputPath(job,outputDir);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
