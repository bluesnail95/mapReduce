package filtering;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.io.*;

/**
 * @Author bluesnail95
 * @Date 2019/7/20 8:25
 * @Description
 */
public class BloomFilterDriver {

    public static void main(String[] args) {
        Path inputFile =  new Path(args[0]);
        int numMembers =  Integer.parseInt(args[1]);
        float falsePosRate = Float.parseFloat(args[2]);
        Path bfFile = new Path(args[3]);

        int vectorSize = BloomFilterUtil.getOptimalBloomFilterSize(numMembers,falsePosRate);
        int nbHash = BloomFilterUtil.getOptimalK(numMembers,vectorSize);
        BloomFilter bloomFilter = new BloomFilter(vectorSize, nbHash, Hash.MURMUR_HASH);

        System.out.println("Training Bloom filter of size " + vectorSize + " with " + nbHash + " hash functions, " + numMembers
                      + " approximate number of records, and " + falsePosRate + " false positive rate ");

        String line = null;
        int numElements = 0;
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            for(FileStatus status:fs.listStatus(inputFile)) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(new FSDataInputStream(fs.open(status.getPath()))));

                System.out.println("Reading " + status.getPath());
                while((line = reader.readLine()) != null) {
                    bloomFilter.add(new Key(line.getBytes()));
                    ++numElements;
                }
            }
            fs.close();
            System.out.println("Trained Bloom filter with " + numElements + " entries");
            System.out.println("Serializing Bloom filter to HDFS at " + bfFile);
            FSDataOutputStream strm = fs.create(bfFile);
            bloomFilter.write(strm);
            strm.flush();
            strm.close();
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
