package util;

import client.entity.BreakRecord;

import java.io.*;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Set;

public class Breakpoint {
    private static final HashMap<String, BreakRecord> breakMap = new HashMap<>();
    private static final File file = new File("StorageBreak.record");
    static  {
        try {
             if (file.exists()) {
                FileInputStream fis = new FileInputStream(file);
                Scanner scanner = new Scanner(fis);
                while (scanner.hasNextLine()){
                    String record = scanner.nextLine();
                    BreakRecord breakRecord = parseRecord(record);
                    breakMap.put(breakRecord.getPath(),breakRecord);
                }
                scanner.close();
                fis.close();
             }
        } catch (IOException e) {throw new RuntimeException(e);}
    }

    public static BreakRecord getBreak(String filePath){
        return breakMap.get(filePath);
    }

    public static void saveBreak(String path, String nodeIp, int nodePort, String resourceId) throws IOException {
        BreakRecord breakRecord = new BreakRecord(path, nodeIp, String.valueOf(nodePort), resourceId);
        breakMap.put(path,breakRecord);
        flushFile();
    }

    public static void deleteBreak(String pathKey) throws IOException {
        breakMap.remove(pathKey);
        flushFile();
    }

    private static synchronized void flushFile() throws IOException {
        FileOutputStream fos = new FileOutputStream(file);
        StringBuilder builder = new StringBuilder();
        Set<String> set = breakMap.keySet();
        for (String next : set) {
            BreakRecord breakRecord = breakMap.get(next);
            builder.append(breakRecord.toString());
        }
        fos.write(builder.toString().getBytes());
        fos.close();
    }

    private static BreakRecord parseRecord(String recordStr){
        String[] split = recordStr.split("——");
        return new BreakRecord(split[0],split[1],split[2],split[3]);
    }

}
