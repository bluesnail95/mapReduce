package numericalSummarization;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author bluesnail95
 * @Date 2019/7/14 9:57
 * @Description
 */
public class MinMaxCountData implements Writable {

    private Date createDate;

    private String userId;

    private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    public MinMaxCountData() {
    }

    public MinMaxCountData(Date createDate, String userId) {
        this.createDate = createDate;
        this.userId = userId;
    }

    public Date getCreateDate() {
        return createDate;
    }

    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(createDate.getTime());
        dataOutput.writeBytes(userId);
    }

    public void readFields(DataInput dataInput) throws IOException {
        createDate = new Date(dataInput.readLong());
        userId = dataInput.readLine();
    }

    @Override
    public String toString() {
        return "MinMaxCountData{" +
                "createDate=" + createDate +
                ", userId='" + userId + '\'' +
                '}';
    }
}
