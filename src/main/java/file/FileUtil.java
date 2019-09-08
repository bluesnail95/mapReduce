package file;

import java.io.File;

/**
 * @Author bluesnail95
 * @Date 2019/7/14 23:31
 * @Description
 */
public class FileUtil {

    /**
     * 删除文件
     * @param fileName 文件名称
     */
    public static void deleteFile(String fileName) {
        File file = new File(fileName);
        if(!file.exists()) {
            return;
        }
        if(file.isFile()) {
            file.delete();
        }else if(file.isDirectory()) {
            File[] fileList = file.listFiles();
            for (int i = 0; i < fileList.length; i++) {
                fileList[i].delete();
            }
            file.delete();
        }
    }
}
