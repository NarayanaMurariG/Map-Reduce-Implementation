package mapreduce;

import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Utils {

    public static long getTotalNoOfLines(String filePath) {
        long totalNoOfLines = -1;
        try {
            Path path = Paths.get(filePath);
            totalNoOfLines = Files.lines(path).count();
        }catch (Exception e){
            System.out.println("Exception Occoured while extracting file size");
        }
        return totalNoOfLines;
    }
}
