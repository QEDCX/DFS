package util;

import message.Ack;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

/**
 * udp发送和接收数据
 * @create 2023/6/24 13:33
 **/
public class UdpService {
    private SliderWindow sliderWindow;

    public SliderWindow getSliderWindow() {
        return sliderWindow;
    }

    public void sendFile(String absolutePath, InetSocketAddress serverUdpAddr) throws IOException, InterruptedException {
        File file = new File(absolutePath);
        sendFile(absolutePath,serverUdpAddr,0,file.length(),null);
    }

    /**
     *
     * @param absolutePath  发送文件的路径
     * @param serverUdpAddr 对方的udp地址
     * @param offset 发送的起始位置
     * @param size   发送的字节数
     * @throws IOException
     * @throws InterruptedException
     */
    public void sendFile(String absolutePath,InetSocketAddress serverUdpAddr,long offset,long size,AtomicLong totalTask) throws IOException, InterruptedException {
        long remainingSize = size;
        //准备发送
        int dataSize = 60 * 1024;
        //文件总包数
        int packNum = (int) (size % dataSize == 0 ? size/dataSize : size/dataSize+1);
        System.out.println("总包数："+packNum);
        //打开文件并跳过偏移量
        FileChannel fc = new RandomAccessFile(absolutePath,"r").getChannel();
        fc.position(offset);

        //包序号，short最大编号文件不能超过2GB，故选int
        int seq = 0;

        CRC32 crc32 = new CRC32();
        //滑动窗口，用于管理包发送、确认、重发
        sliderWindow = new SliderWindow(serverUdpAddr);

        System.out.println("开始发送数据");
        long begT = System.currentTimeMillis();
        while (seq < packNum){              //文件未读完
            //发送窗口有容量时添加数据
            while (seq < packNum && sliderWindow.hasSpace()){
                ByteBuffer packetBuffer;
                byte[] crcTmpData;          //生成CRC的文件数据
                //若是最后一个包，余量不足60KB
                if (seq == packNum-1) {
                    //读取剩余的字节数
                    int num = (int)(size % dataSize);
                    packetBuffer = ByteBuffer.allocate(4 + 8 + num);
                    crcTmpData = new byte[num];
                    if (totalTask != null) totalTask.addAndGet(-num);            //减去任务量
                    remainingSize -= num;
                }
                else {
                    packetBuffer = ByteBuffer.allocate(4 + 8 + dataSize);
                    crcTmpData = new byte[dataSize];
                    if (totalTask != null) totalTask.addAndGet(-dataSize);       //减去任务量
                    remainingSize -= dataSize;
                }
                packetBuffer.putInt(0,seq);         //添加序号
                packetBuffer.position(12);     //改变缓冲区位置到12准备存数据
                fc.read(packetBuffer);                    //存数据
                //取出数据生成CRC并存入buffer
                packetBuffer.position(12);      //回到12准备取数据
                packetBuffer.get(crcTmpData,0,crcTmpData.length);
                crc32.update(crcTmpData);
                long crc = crc32.getValue();
                crc32.reset();                       //重置crc
                packetBuffer.putLong(4,crc);
                //构造packet并添加到滑动窗口
                byte[] packetBytes = packetBuffer.array();
                DatagramPacket packet = new DatagramPacket(packetBytes,packetBytes.length,serverUdpAddr);
                sliderWindow.addPacket(seq,packet);
                seq += 1;               //序号加一
                packetBuffer.clear();
            }
            //发送窗口数据
            if (!sliderWindow.send()){
                System.out.println("UDP服务信息---传输中断！");
                if (totalTask != null) totalTask.addAndGet(-remainingSize);
                break;
            }
            Thread.sleep(200);   //降低发送速度
        }
        long endT = System.currentTimeMillis();
        float time = (endT - begT)*1.0f/1000;
        System.out.println("发送用时："+time+" 秒\n发送速率："+size/1024/1024/time+" MB/s\n");
        fc.close();
    }



    public long receiveFile(DatagramSocket socket, String storagePath, long size, AtomicLong totalTask) throws IOException {
        FileOutputStream fos = new FileOutputStream(storagePath,true);  //追加模式打开
        socket.setReceiveBufferSize(1024*1024*5);
        DatagramPacket rcvPacket = new DatagramPacket(new byte[61*1024],61*1024);  //接收缓冲区

        long rcvSize = 0;                                               //统计接收总数以判断是否接收完毕
        int requireSeq = 0;                                             //下一个接收的序号，用于保证按序写入磁盘
        HashMap<Integer,byte[]> packetCache = new HashMap<>();          //缓存序号不对的数据
        DatagramPacket ackPacket;      //确认包

        byte[] seq;                    //包序号
        byte[] check;                  //crc校验字段
        byte[] data;                   //数据

        System.out.println("开始接收...");
        long beginT = System.currentTimeMillis();
        socket.setSoTimeout(7000);      //接收超时7s
        //未接收完时
        while (rcvSize < size){
            //收包
            try {
                socket.receive(rcvPacket);
            }catch (IOException e) {
                //减去剩下还没接收的字节
                totalTask.addAndGet(-(size - rcvSize));
                System.out.println("接收超时！已停止");break;
            }

            //字段提取
            seq = Arrays.copyOfRange(rcvPacket.getData(),0,4);
            check = Arrays.copyOfRange(rcvPacket.getData(),4,12);
            data = Arrays.copyOfRange(rcvPacket.getData(),12,rcvPacket.getLength());
            //校验通过,不通过则不理会，等待重发
            if (verify(data,check)){
                //发送确认包
                Ack ackMsg = new Ack();
                ackMsg.ack = bytesToInt(seq);
                //序列化并发送
                byte[] ackBytes = Serialize.serialize(ackMsg);
                ackPacket = new DatagramPacket(ackBytes,ackBytes.length,rcvPacket.getSocketAddress());
                socket.send(ackPacket);
                System.out.println("send-ack:"+ackMsg.ack);               //打印发送的确认号

                //包按序到达
                if (ackMsg.ack == requireSeq){
                    rcvSize += data.length;                               //更新接收的字节数
                    if (totalTask != null) totalTask.addAndGet(-data.length);// totalTask -= rcvSize;         //服务端接收时减去任务量
                    fos.write(data);                                      //写入磁盘
                    fos.flush();
                    requireSeq += 1;                                      //更新下一次需要的包序号
                }
                //防止被重发的数据包延迟到达，因此要大于
                else if (ackMsg.ack > requireSeq){
                    rcvSize += data.length;                               //更新接收的字节数
                    if (totalTask != null)  totalTask.addAndGet(-data.length);//totalTask -= rcvSize;         //服务端接收时减去任务量
                    //先去缓存中找
                    byte[] bytes = packetCache.get(requireSeq);
                    //缓存中有
                    if (bytes != null){
                        System.out.println("取出缓存,序号："+requireSeq);
                        fos.write(data);                                  //写入磁盘
                        fos.flush();
                        packetCache.remove(requireSeq);                   //从缓存删去
                        requireSeq += 1;                                  //更新下一次需要的包序号
                    }
                    packetCache.put(ackMsg.ack,data);                     //不按序到达的放入缓存
                    System.out.println("存入缓存,序号："+ackMsg.ack);
                }
            }else {
                System.out.println("校验错误,序号："+bytesToInt(seq));
            }
        } //while
        System.out.println("\n");
        //清空缓存
        if (packetCache.size() != 0) {
            System.out.print("缓存余量："+packetCache.size());
            while (packetCache.size() != 0){
                byte[] bs = packetCache.get(requireSeq);
                fos.write(bs);
                fos.flush();
                packetCache.remove(requireSeq);
                requireSeq += 1;
            }
            System.out.println("  ...  已写入磁盘");
        }
        fos.close();
        socket.close();
        float time = (System.currentTimeMillis() - beginT) * 1.0f / 1000;
        System.out.println("接收用时："+time+" 秒");
        System.out.println("接收字节："+rcvSize+" | 接收百分比：" + rcvSize*1.0f / size * 100+" %");
        System.out.println("接收速率："+size/1024/1024 / time+" MB/s\n");

        return rcvSize;
    }

    public Long receiveFileTest(DatagramSocket socket, String storagePath,Long offset,long size, AtomicLong totalTask) throws IOException {
        FileChannel channel = new RandomAccessFile(storagePath,"rw").getChannel();
        channel.position(offset);
        socket.setReceiveBufferSize(1024*1024*5);
        DatagramPacket rcvPacket = new DatagramPacket(new byte[61*1024],61*1024);  //接收缓冲区

        long rcvSize = 0;                                               //统计接收总数以判断是否接收完毕
        int requireSeq = 0;                                             //下一个接收的序号，用于保证按序写入磁盘
        HashMap<Integer,byte[]> packetCache = new HashMap<>();          //缓存序号不对的数据
        DatagramPacket ackPacket;      //确认包

        byte[] seq;                    //包序号
        byte[] check;                  //crc校验字段
        byte[] data;                   //数据

        System.out.println("开始接收...");
        long beginT = System.currentTimeMillis();
        socket.setSoTimeout(7000);      //接收超时7s
        //未接收完时
        while (rcvSize < size){
            //收包
            try {
                socket.receive(rcvPacket);
            }catch (IOException e) {
                //减去剩下还没接收的字节
                totalTask.addAndGet(-(size - rcvSize));
                System.out.println("接收超时！已停止");break;
            }

            //字段提取
            seq = Arrays.copyOfRange(rcvPacket.getData(),0,4);
            check = Arrays.copyOfRange(rcvPacket.getData(),4,12);
            data = Arrays.copyOfRange(rcvPacket.getData(),12,rcvPacket.getLength());
            //校验通过,不通过则不理会，等待重发
            if (verify(data,check)){
                //发送确认包
                Ack ackMsg = new Ack();
                ackMsg.ack = bytesToInt(seq);
                //序列化并发送
                byte[] ackBytes = Serialize.serialize(ackMsg);
                ackPacket = new DatagramPacket(ackBytes,ackBytes.length,rcvPacket.getSocketAddress());
                socket.send(ackPacket);
                System.out.println("send-ack:"+ackMsg.ack);               //打印发送的确认号

                //包按序到达
                if (ackMsg.ack == requireSeq){
                    rcvSize += data.length;                               //更新接收的字节数
                    if (totalTask != null) totalTask.addAndGet(-data.length);// totalTask -= rcvSize;         //服务端接收时减去任务量
                    channel.write(ByteBuffer.wrap(data));                                      //写入磁盘
                    requireSeq += 1;                                      //更新下一次需要的包序号
                }
                //防止被重发的数据包延迟到达，因此要大于
                else if (ackMsg.ack > requireSeq){
                    rcvSize += data.length;                               //更新接收的字节数
                    if (totalTask != null)  totalTask.addAndGet(-data.length);//totalTask -= rcvSize;         //服务端接收时减去任务量
                    //先去缓存中找
                    byte[] bytes = packetCache.get(requireSeq);
                    //缓存中有
                    if (bytes != null){
                        System.out.println("取出缓存,序号："+requireSeq);
                        channel.write(ByteBuffer.wrap(bytes));                                //写入磁盘
                        packetCache.remove(requireSeq);                   //从缓存删去
                        requireSeq += 1;                                  //更新下一次需要的包序号
                    }
                    packetCache.put(ackMsg.ack,data);                     //不按序到达的放入缓存
                    System.out.println("存入缓存,序号："+ackMsg.ack);
                }
            }else {
                System.out.println("校验错误,序号："+bytesToInt(seq));
            }
        } //while
        System.out.println("\n");
        //清空缓存
        if (packetCache.size() != 0) {
            System.out.print("缓存余量："+packetCache.size());
            while (packetCache.size() != 0){
                byte[] bs = packetCache.get(requireSeq);
                channel.write(ByteBuffer.wrap(bs));
                packetCache.remove(requireSeq);
                requireSeq += 1;
            }
            System.out.println("  ...  已写入磁盘");
        }
        channel.close();
        socket.close();
        float time = (System.currentTimeMillis() - beginT) * 1.0f / 1000;
        System.out.println("接收用时："+time+" 秒");
        System.out.println("接收字节："+rcvSize+" | 接收百分比：" + rcvSize*1.0f / size * 100+" %");
        System.out.println("接收速率："+size/1024/1024 / time+" MB/s\n");

        return rcvSize;
    }

    private boolean verify(byte[] rcvData,byte[] rcvCheck){
        CRC32 crc32 = new CRC32();
        crc32.update(rcvData);
        return  bytesToLong(rcvCheck) == crc32.getValue();
    }

    private int bytesToInt(byte[] bytes){
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        return buf.getInt();
    }
    private long bytesToLong(byte[] bytes){
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        return buf.getLong();
    }

}
