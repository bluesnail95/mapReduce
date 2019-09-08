package numericalSummarization;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author bluesnail95
 * @Date 2019/7/14 21:51
 * @Description
 */
public class CountAverageData implements Writable {

    //日期
    private Date creationDate;

    //文本
    private String text;

    private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    public CountAverageData() {
    }

    public CountAverageData(Date creationDate, String text) {
        this.creationDate = creationDate;
        this.text = text;
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBytes(frmt.format(creationDate));
        dataOutput.writeBytes(text);
    }

    public void readFields(DataInput dataInput) throws IOException {
        try {
            System.out.println(dataInput);
            creationDate = frmt.parse(dataInput.toString());
            text = dataInput.readLine();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "{" +
                "creationDate=" + creationDate +
                ", text='" + text + '\'' +
                '}';
    }
}
