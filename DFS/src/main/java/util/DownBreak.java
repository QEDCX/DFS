package util;

import client.entity.DownBreakRecord;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class DownBreak {
    private static final File file = new File("DownBreak.record");
    private static final Map<String, DownBreakRecord> map = new HashMap<>();

    static {
        try {
                if (file.exists()){
                    Scanner scanner = new Scanner(file);
                    while (scanner.hasNextLine()){
                        String[] strings = scanner.nextLine().split("——");
                        map.put(strings[0],new DownBreakRecord(strings[0],strings[1],Integer.parseInt(strings[2]),Long.parseLong(strings[3]),strings[4]));
                    }
                    scanner.close();
                }
        } catch (Exception e) {
            System.out.println(e);
        }
    }


    public static void saveRecord(DownBreakRecord record)  {
        map.put(record.resourceId,record);
        flushFile();
    }

    public static void deleteRecord(String resId)  {
        map.remove(resId);
        flushFile();
    }

    public static DownBreakRecord getRecord(String resId)  {
        return map.get(resId);
    }

    public static synchronized void flushFile()  {
        try {
            FileOutputStream fos = new FileOutputStream(file);
            StringBuilder builder = new StringBuilder();
            Collection<DownBreakRecord> values = map.values();
            for (DownBreakRecord value : values) {
                builder.append(value);
            }
            fos.write(builder.toString().getBytes());
            fos.close();
        } catch (IOException e) {
            System.out.println("下载断点文件刷新失败："+e.getMessage());
        }
    }

}
