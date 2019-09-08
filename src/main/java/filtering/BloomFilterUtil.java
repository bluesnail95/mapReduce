package filtering;

/**
 * @Author bluesnail95
 * @Date 2019/7/20 16:19
 * @Description
 */
public class BloomFilterUtil {

    public static int getOptimalBloomFilterSize(int numMembers,float falsePosRate) {
        int size = (int) (-numMembers * (float) Math.log(falsePosRate) /Math.pow(Math.log(2) , 2));
        return size;
    }

    public static int getOptimalK(float numMembers,float vectorSize) {
        return (int)Math.round(vectorSize / numMembers * Math.log(2));
    }

}
