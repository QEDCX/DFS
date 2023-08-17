package client;

import client.entity.BreakRecord;
import client.entity.DownBreakRecord;
import message.AbstractMessage;
import message.LocationMessage;
import message.client.DownMessage;
import message.client.StorageBreak;
import message.client.StorageMessage;
import message.client.BreakDownload;
import message.datanode.BreakStorageResponse;
import message.datanode.ResourceId;
import message.datanode.ServerUdpRcvPort;
import util.Breakpoint;
import util.DownBreak;
import util.Serialize;
import util.UdpService;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Objects;


public class ClientAPI {
    public static String StorageFile(String ip,int port,String absolutePath) throws IOException, InterruptedException {
        //检查是否为断点
        BreakRecord breakRecord = Breakpoint.getBreak(absolutePath);
        //不是断点
        if (breakRecord == null)  return StorageFullFile(ip,port,absolutePath);
        else return StorageBreakPoint(breakRecord);
    }


    private static String StorageBreakPoint(BreakRecord breakRecord) throws IOException, InterruptedException {
        //TCP直连服务器
        String path = breakRecord.getPath();
        String ip = breakRecord.getNodeIp();
        int port = Integer.parseInt(breakRecord.getNodePort());
        String resourceId = breakRecord.getResourceId();
        Socket socket = new Socket(ip,port);

        //发送信息
        StorageBreak storageBreak = new StorageBreak(resourceId);

        socket.getOutputStream().write(Objects.requireNonNull(Serialize.serialize(storageBreak)));

        //接收响应
        BreakStorageResponse resMsg = null;
        AbstractMessage msg = Serialize.deserialize(socket.getInputStream());
        if (msg.code == AbstractMessage.Break_Storage_Response) resMsg = (BreakStorageResponse) msg;
        else if (msg.code == AbstractMessage.ERROR) throw new RuntimeException("该文件不是断点");
        else throw new RuntimeException("响应码错误:"+msg.code);
        long offset = resMsg.offset;
        int rcvPort = resMsg.rcvPort;
        socket.close();
        //发送剩余数据
        UdpService udpService = new UdpService();
        InetSocketAddress nodeAddr = new InetSocketAddress(ip, rcvPort);
        long len = new File(path).length() - offset;
        udpService.sendFile(path,nodeAddr,offset,len,null);
        //若发送完毕则删除断点
        Breakpoint.deleteBreak(path);
        return resourceId;
    }
    private static String StorageFullFile(String ip,int port,String absolutePath)  {
        try {
            //发送请求
            Socket socketToDis = new Socket(ip,port);
            File file = new File(absolutePath);
            if (!file.exists()){throw new RuntimeException("无文件");}
            long size = file.length();
            String fileName = file.getName();

            StorageMessage storageMessage = new StorageMessage(fileName, size);
            socketToDis.getOutputStream().write(Objects.requireNonNull(Serialize.serialize(storageMessage)));

            //获取存储的数据节点地址并关闭TCP连接
            LocationMessage message = (LocationMessage)Serialize.deserialize(socketToDis.getInputStream());
            InetSocketAddress nodeEndPoint = message.nodeEndPoint;
            socketToDis.close();

            //连接数据节点并发送TCP存储请求
            Socket socketToData = new Socket();
            socketToData.connect(nodeEndPoint);
            socketToData.getOutputStream().write(Objects.requireNonNull(Serialize.serialize(storageMessage)));

            //获取server端udp接收端口后构造服务端UDP地址
            int ServerUdpRcvPort  = ((ServerUdpRcvPort) Objects.requireNonNull(Serialize.deserialize(socketToData.getInputStream()))).port;
            InetSocketAddress serverUdpAddr = new InetSocketAddress(nodeEndPoint.getAddress(),ServerUdpRcvPort);

            //获取分配的resId
            String resourceId  = ((ResourceId) Objects.requireNonNull(Serialize.deserialize(socketToData.getInputStream()))).resourceId;
            socketToData.close();

            //将发送保存为断点
            Breakpoint.saveBreak(absolutePath, String.valueOf(nodeEndPoint.getAddress().getHostAddress()),nodeEndPoint.getPort(),resourceId);
            //发送文件
            UdpService udpService = new UdpService();
            udpService.sendFile(absolutePath,serverUdpAddr);

            //删除断点
            Breakpoint.deleteBreak(absolutePath);
            return resourceId;
        } catch (Exception e){
            System.out.println(e);
        }
        return null;
    }

    public static void DownFile(String ip,int port,String resourceId,String storageDir){
        DownBreakRecord record = DownBreak.getRecord(resourceId);
        //不是断点
        if (record == null) DownFullFile(ip,port,resourceId,storageDir);
        else {
            long fileSize = new File(record.storagePath).length();
            //防止下载完后没有执行完删除断点的代码，因此再检查一遍
            if (fileSize != record.totalSize){
                DownBreakFile(resourceId);
            }else {
                DownBreak.deleteRecord(resourceId);
            }
        }
    }

    private static void DownBreakFile(String resourceId){
        try{
            Socket socket = new Socket();
            DatagramSocket udpSocket = new DatagramSocket();
            DownBreakRecord record = DownBreak.getRecord(resourceId);
            InetSocketAddress addr = new InetSocketAddress(record.ip,record.port);
            socket.connect(addr);
            OutputStream os = socket.getOutputStream();
            InputStream is = socket.getInputStream();
            //发送续下载请求
            long rcvSize = new File(record.storagePath).length();
            os.write(Objects.requireNonNull(Serialize.serialize(new BreakDownload(resourceId, rcvSize, udpSocket.getLocalPort()))));
            AbstractMessage res = Serialize.deserialize(is);
            //可以接收文件
            assert res != null;
            if (res.code == AbstractMessage.OK){
                UdpService udpService = new UdpService();
                DownBreakRecord breakRecord = DownBreak.getRecord(resourceId);                  //下载断点记录
                long remainingSize = breakRecord.totalSize - rcvSize;                           //剩余要接收的字节数
                //开始接收文件
                long receivedSize = udpService.receiveFile(udpSocket, breakRecord.storagePath, remainingSize, null);
                if (receivedSize == remainingSize) DownBreak.deleteRecord(resourceId);          //接收完则删除断点
                else System.out.println("接收未完成！");
            }else {
                System.out.println("断点下载异常！");
            }
        }catch (Exception e){
            System.out.println(e);
        }
    }

    private static void DownFullFile(String ip,int port,String reId,String storageDir)  {
        try{

            //向Dis发送携带资源id的Down 信息询问数据存储位置等信息
            Socket socketToDis = new Socket(ip,port);
            DownMessage downMessage = new DownMessage(reId);
            socketToDis.getOutputStream().write(Objects.requireNonNull(Serialize.serialize(downMessage)));

            //获取LocationMessage,即文件的存储信息,在此对象上修改code和udp接收端口，然后发给data node
            DatagramSocket udpSocket = new DatagramSocket();
            LocationMessage locationMsg = (LocationMessage)Serialize.deserialize(socketToDis.getInputStream());
            assert locationMsg != null;
            locationMsg.setLocationMessageToDataNode(udpSocket.getLocalPort());  //设置接收的udp端口、将请求码从OK变为DOWN，发给服务端
            socketToDis.close();

            //向数据节点发送下载请求
            Socket socketToData = new Socket();
            socketToData.connect(locationMsg.nodeEndPoint);
            socketToData.getOutputStream().write(Objects.requireNonNull(Serialize.serialize(locationMsg)));
            socketToData.close();

            //接收文件
            String path = storageDir + File.separator + locationMsg.fileName;                        //存储路径
            DownBreak.saveRecord(new DownBreakRecord(reId,locationMsg.nodeEndPoint.getAddress().getHostAddress(),locationMsg.nodeEndPoint.getPort(), locationMsg.size,path));
            UdpService udpService = new UdpService();
            long rcvSize = udpService.receiveFile(udpSocket,path, locationMsg.size, null);  //开始接收
            if (rcvSize == locationMsg.size) DownBreak.deleteRecord(reId);                          //接收完毕则删除断点，否则更新
        }catch (IOException e){
            System.out.println(e);
        }

    }
}
