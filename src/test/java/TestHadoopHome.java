import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author bluesnail95
 * @Date 2019/7/13 10:59
 * @Description
 */
public class TestHadoopHome {

    public static void main(String[] args) {
//       System.out.println(System.getenv("HADOOP_HOME"));
//
//       SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
//        try {
//            Date formatDate = frmt.parse("2019-07-14T20:20:28.599");
//            Calendar calendar = Calendar.getInstance();
//            calendar.setTime(formatDate);
//            System.out.println(calendar.getTime().toString());
//            System.out.println(calendar.get(Calendar.HOUR_OF_DAY));
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }

//        Pattern pattern = Pattern.compile("^[a-zA-Z]+$");
//        Matcher matcher = pattern.matcher("123");
//        System.out.println(matcher.matches());
//        System.out.println();

        String test = "This is a test";
        String[] splits = test.split("\\s");
        for (int i = 0; i < splits.length; i++) {
            System.out.println(splits[i]);
        }
    }
}
