package util;

import message.AbstractMessage;

import java.io.*;

public class Serialize {
    public static byte[] serialize(Object ob) {
        try {
            ByteArrayOutputStream bytesStream = new ByteArrayOutputStream();
            ObjectOutputStream obStream = new ObjectOutputStream(bytesStream);
            obStream.writeObject(ob);
            obStream.flush();
            obStream.close();
            return bytesStream.toByteArray();
        }catch (IOException e){
            e.printStackTrace();
            System.out.println("序列化异常："+e.getMessage());
        }catch (Exception e){
            System.out.println("异常"+e.getMessage());
        }

        return null;
    }


    public static AbstractMessage deserialize(InputStream inputStream) {
        try {
            ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
            AbstractMessage msg = (AbstractMessage) objectInputStream.readObject();
            return msg;
        }catch (IOException | ClassNotFoundException e){
            System.out.println("反序列化异常："+e);
        }catch (Exception e){
            System.out.println("异常"+e);
        }
        return null;
    }

    public static AbstractMessage deserialize(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
            AbstractMessage msg = (AbstractMessage) objectInputStream.readObject();
            objectInputStream.close();
            byteArrayInputStream.close();
            return msg;
        }catch (IOException | ClassNotFoundException e){
            System.out.println("反序列化异常："+e);
            e.printStackTrace();
        }
        return null;
    }
}
