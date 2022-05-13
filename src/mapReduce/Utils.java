package mapReduce;

import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Utils {
    public static long getFileSize(String filePath){
        long fileSizeInBytes = -1;
        try {
            Path path = Paths.get(filePath);
            fileSizeInBytes = Files.size(path);
        }catch (Exception e){
            System.out.println("Exception Occoured while extracting file size");
        }
        return fileSizeInBytes;
    }

    //https://stackoverflow.com/questions/37702667/how-to-get-file-bytes-in-a-given-range
    public static StringBuilder getFileDataInRange(long start,long end,String filePath){
        StringBuilder builder = new StringBuilder();
        try{
            RandomAccessFile file = new RandomAccessFile(filePath,"r");

            byte[] bytes = new byte[(int) (end-start)];

            file.read(bytes,(int) start,(int) (end - start));

            builder.append(new String(bytes, StandardCharsets.UTF_8));

        }catch (Exception e){
            System.out.println("Exception occured while reading file segment");
            e.printStackTrace();
        }
        return builder;
    }

//    https://gist.github.com/kabronkline/e9f2c0fcad02c69c3212
//    public static int breakAtIndex(String filePath){
//
//    }

}
