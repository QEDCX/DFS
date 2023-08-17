package DataNode;

import client.entity.OffsetRecord;
import message.*;
import message.client.BreakDownload;
import message.client.DownMessage;
import message.client.StorageBreak;
import message.client.StorageMessage;
import message.datanode.*;
import message.namenode.*;
import util.Reconnect;
import util.Serialize;
import util.UdpService;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

public class Worker {
    private final Socket socketToDis = new Socket();
    private Socket socketToMeta = new Socket();
    private final ExecutorService executorService = Executors.newFixedThreadPool(50);
    private final ScheduledExecutorService scheduledStateSend = Executors.newScheduledThreadPool(1);
    private final List<FileState> fileStates = new ArrayList<>();
    private final Map<Integer,OffsetRecord> offsetMap = new HashMap<>();
    private Integer Id;
    private int listenPort;
    private final AtomicLong totalTask = new AtomicLong(0);
    private final Object lock = new Object();
    /**
     * 写写互斥
     * @create 2023/6/22 14:58
     **/
    static class FileState{
        String path;
        boolean writing = false;
        public FileState(String path) {
            this.path = path;
        }
    }


    /**
     * 满足写写互斥时返回文件路径
     * 写操作执行完后需要将state的状态置0
     * @create 2023/6/22 15:18
     **/
    private FileState getFileStateForWrite(){
        //则返回非写状态的文件路径
        for (FileState fileState:fileStates){
            if (!fileState.writing){
                fileState.writing = true;
                return fileState;
            }
        }
        //文件都处于写状态时会返回
        return null;
    }


    public void app_run(int listenPort,String remoteIp,int remotePort) {
        try {
            this.listenPort = listenPort;
            ini(remoteIp,remotePort);
            handler();
        }catch (IOException e){
            e.printStackTrace();
        }catch (Exception e){
            System.out.println(e);
        }

    }


    /**
     * 创建count个文件用于存储
     * @create 2023/6/22 13:22
     **/
    private void iniStorageFiles(int count) throws IOException {
        File dir = new File("StorageDir");
        File record = new File("offset.record");
        if (!record.exists()) record.createNewFile();
        if (!dir.exists()){dir.mkdir();}
        if (!record.exists()){record.createNewFile();}

        String dirPath = dir.getAbsolutePath()+File.separator;
        FileOutputStream fos = new FileOutputStream(record,true);
        for (int i = 1;i<=count;i++){
            String absolutePath = dirPath+i;
            File file = new File(absolutePath);
            if (!file.exists()) {
                if (!file.createNewFile()) throw new RuntimeException("存储文件创建失败！");
                fos.write(new OffsetRecord(i,0L).toString().getBytes());
            }
            fileStates.add(new FileState(absolutePath));
        }
        fos.close();
        //offset.record解析
        Scanner scanner = new Scanner(record);
        while (scanner.hasNextLine()){
            String s = scanner.nextLine();
            String[] split = s.split(":");
            Integer name = Integer.parseInt(split[0]);
            Long offset = Long.parseLong(split[1]);
            offsetMap.put(name,new OffsetRecord(name,offset));
        }
        System.out.println("存储文件解析完毕!\n文件偏移解析完毕!");
    }
    /**
     * 判断有无ID，注册到 Dis 和 meta
     * @create 2023/6/20 10:18
     **/
    private void ini(String remoteIp,int port) throws IOException {
        System.out.println("正在初始化...");
        //初始化存储文件
        iniStorageFiles(50);

        //判断有无ID
        File file = new File("data_node_id.txt");
        if (file.exists()){
            Scanner scanner = new Scanner(new FileInputStream(file));
            Id = Integer.parseInt(scanner.nextLine());
        }else {
            boolean success = file.createNewFile();
            if (!success) throw new RuntimeException("data_node_id.txt 文件创建失败！");
        }

        //注册到 dis
        System.out.print("注册到Dispatcher: ");
        boolean isAllocateId = Id == null;
        //注册消息
        DataRegisterMessage msg = new DataRegisterMessage(isAllocateId,Id,listenPort);
        //发送消息并接收响应
        InetSocketAddress dispatcherAddr = new InetSocketAddress(remoteIp, port);
        socketToDis.connect(dispatcherAddr);
        socketToDis.getOutputStream().write(Objects.requireNonNull(Serialize.serialize(msg)));
        ResponseMessage res = (ResponseMessage) Serialize.deserialize(socketToDis.getInputStream());
        InetSocketAddress metaAddr;
        if (res.code == AbstractMessage.OK){
            if (isAllocateId){
                Id = res.allocatedDataNodeId;       //获取分配的Id
                FileOutputStream fos = new FileOutputStream("data_node_id.txt");
                fos.write((String.valueOf(Id)).getBytes());
                fos.close();
            }
            metaAddr = res.nameNodeEndPoint;
            System.out.println("注册成功");
            scheduleSendState(4,TimeUnit.SECONDS);
        }else {
            System.out.println("注册失败\n初始化失败");
            return;
        }


        //注册到 meta
        System.out.print("注册到MetaNode: ");
        socketToMeta.connect(metaAddr);
        //接收 注册结果
        AbstractMessage nres = Serialize.deserialize(socketToMeta.getInputStream());
        if (nres.code == AbstractMessage.OK){
            System.out.println("注册成功");
        }else{
            System.out.println("注册失败\n初始化失败");
            return;
        }
        System.out.println("初始化成功\n");
    }
    private void handler() throws IOException, InterruptedException {
        System.out.println("等待客户端请求...\n");
        Selector selector = Selector.open();
        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.bind(new InetSocketAddress("0.0.0.0",listenPort));
        ssc.configureBlocking(false);
        ssc.register(selector, SelectionKey.OP_ACCEPT);
        while (true){
            selector.select();
            //返回迭代器
            Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
            while (iterator.hasNext()){
                SelectionKey key = iterator.next();
                //客户端连接,注册到选择器
                if (key.isAcceptable()){
                    SocketChannel socketChannel = ssc.accept();
                    System.out.println("连接请求："+socketChannel.getRemoteAddress());
                    socketChannel.configureBlocking(false);
                    socketChannel.register(selector,SelectionKey.OP_READ);
                }
                //处理客户端请求
                else if (key.isReadable()){
                    ByteBuffer buffer = ByteBuffer.allocate(1024);
                    byte[] msgBytes;        //存储读取到的字节流
                    SocketChannel socketChannel = (SocketChannel)key.channel();
                    socketChannel.read(buffer);
                    buffer.flip();
                    msgBytes = new byte[buffer.remaining()];        //分配空间
                    buffer.get(msgBytes);       //读取到数组
                    buffer.clear();
                    //反序列化消息流
                    AbstractMessage msg = Serialize.deserialize(msgBytes);
                    switch (Objects.requireNonNull(msg).code){
                        //处理客户端下载请求
                        case AbstractMessage.DOWN:
                            System.out.println("处理请求：下载 | 客户端："+socketChannel.getRemoteAddress());
                            //检查连接
                            checkConnect(socketToMeta,()->{
                                try {
                                    //查询meta地址
                                    socketToDis.getOutputStream().write(Objects.requireNonNull(Serialize.serialize(new QueryMetaAddress())));
                                    AbstractMessage res = Serialize.deserialize(socketToDis.getInputStream());
                                    if (res.code == AbstractMessage.RESPONSE_META_ADDRESS){
                                        InetSocketAddress metaAddress = ((MetaAddressResponse) res).metaAddress;
                                        socketToMeta.close();
                                        socketToMeta = new Socket();
                                        socketToMeta.connect(metaAddress);
                                        //接收响应
                                        AbstractMessage message = Serialize.deserialize(socketToMeta.getInputStream());
                                        if (message.code != AbstractMessage.OK) throw new RuntimeException("注册至meta失败");
                                    }else if (res.code == AbstractMessage.ERROR){
                                        System.out.println("元数据节点离线！");
                                        return false;
                                    }else {
                                        System.out.println("异常响应码！");
                                        return false;
                                    }
                                    return true;
                                } catch (Exception e) {
                                    return false;
                                }
                            });
                            InetAddress Ip = socketChannel.socket().getInetAddress();
                            executorService.submit(()-> {
                                try {handleDownMsg(Ip,msg);} catch (IOException e) {System.out.println("handleDown异常："+e);} catch (InterruptedException e) {System.out.println("handleDown异常："+e);}
                                System.out.println("\n等待客户端请求...");
                            });
                            break;

                        //处理客户端存储请求
                        case AbstractMessage.STORAGE:
                            System.out.println("处理请求：存储 | 客户端："+socketChannel.getRemoteAddress());
                            //获取接收的文件名和大小
                            String fileName = ((StorageMessage)msg).fileName;
                            long size = ((StorageMessage)msg).size;
                            //检查连接
                            checkConnect(socketToMeta,()->{
                                try {
                                    //查询meta地址
                                    socketToDis.getOutputStream().write(Objects.requireNonNull(Serialize.serialize(new QueryMetaAddress())));
                                    AbstractMessage res = Serialize.deserialize(socketToDis.getInputStream());
                                    if (res.code == AbstractMessage.RESPONSE_META_ADDRESS){
                                        InetSocketAddress metaAddress = ((MetaAddressResponse) res).metaAddress;
                                        socketToMeta.close();
                                        socketToMeta = new Socket();
                                        socketToMeta.connect(metaAddress);
                                        //接收响应
                                        AbstractMessage message = Serialize.deserialize(socketToMeta.getInputStream());
                                        if (message.code != AbstractMessage.OK) throw new RuntimeException("注册至meta失败");
                                    }else if (res.code == AbstractMessage.ERROR){
                                        System.out.println("元数据节点离线！");
                                        return false;
                                    }else {
                                        System.out.println("异常响应码！");
                                        return false;
                                    }
                                    return true;
                                } catch (Exception e) {
                                    return false;
                                }
                            });
                            //返回Udp接收端口
                            DatagramSocket socket = new DatagramSocket();
                            ServerUdpRcvPort udpRcvPort = new ServerUdpRcvPort();
                            udpRcvPort.port = socket.getLocalPort();
                            socketChannel.write(ByteBuffer.wrap(Objects.requireNonNull(Serialize.serialize(udpRcvPort))));
                            //资源id生成
                            String resourceId = UUID.randomUUID().toString();
                            //向客户端返回资源id
                            ResourceId resIdMsg = new ResourceId(resourceId);
                            byte[] serializeResId = Serialize.serialize(resIdMsg);
                            socketChannel.write(ByteBuffer.wrap(serializeResId));
                            //开始接收文件
                            executorService.submit(()-> {
                                try {handleStorageMsg(socket,fileName,size,resourceId);} catch (IOException e) {System.out.println("handleStorage异常："+e);}
                                System.out.println("\n等待客户端请求...");
                            });
                            break;

                        //断点续存
                        case AbstractMessage.CONTINUE_STORAGE:
                            System.out.println("处理请求：续存 | 客户端："+socketChannel.getRemoteAddress());
                            StorageBreak storageBreak = (StorageBreak) msg;
                            String resId = storageBreak.resId;
                            //检查连接
                            checkConnect(socketToMeta,()->{
                                try {
                                    //查询meta地址
                                    socketToDis.getOutputStream().write(Objects.requireNonNull(Serialize.serialize(new QueryMetaAddress())));
                                    AbstractMessage res = Serialize.deserialize(socketToDis.getInputStream());
                                    if (res.code == AbstractMessage.RESPONSE_META_ADDRESS){
                                        InetSocketAddress metaAddress = ((MetaAddressResponse) res).metaAddress;
                                        socketToMeta.close();
                                        socketToMeta = new Socket();
                                        socketToMeta.connect(metaAddress);
                                        //接收响应
                                        AbstractMessage message = Serialize.deserialize(socketToMeta.getInputStream());
                                        if (message.code != AbstractMessage.OK) throw new RuntimeException("注册至meta失败");
                                    }else if (res.code == AbstractMessage.ERROR){
                                        System.out.println("元数据节点离线！");
                                        return false;
                                    }else {
                                        System.out.println("异常响应码！");
                                        return false;
                                    }
                                    return true;
                                } catch (Exception e) {
                                    return false;
                                }
                            });
                            //查询断点
                            BreakQuery query = new BreakQuery(resId);
                            socketToMeta.getOutputStream().write(Objects.requireNonNull(Serialize.serialize(query)));
                            //读取响应
                            AbstractMessage res1;
                            res1 = Serialize.deserialize(socketToMeta.getInputStream());
                            if (res1.code == AbstractMessage.Break_QUERY_Response) {
                                BreakQueryResponse res = (BreakQueryResponse) res1;
                                if (res.nodeId.equals(Id)){
                                    String path = res.path;
                                    Long offset = res.offset;   //代表客户端文件发送的起始位置
                                    //获取存储中的偏移
                                    DownMessage downMessage = new DownMessage(resId);
                                    socketToMeta.getOutputStream().write(Objects.requireNonNull(Serialize.serialize(downMessage)));
                                    LocationMessage locationMessage = (LocationMessage)Serialize.deserialize(socketToMeta.getInputStream());
                                    Long storageOffset = locationMessage.offset+offset;     //服务端开始存储的偏移
                                    Long rcvSize = locationMessage.size - offset;           //接收的字节数
                                    //返回udp接收端口和offset
                                    DatagramSocket udpRcvSocket = new DatagramSocket();
                                    BreakStorageResponse Response = new BreakStorageResponse(offset, udpRcvSocket.getLocalPort());
                                    byte[] bytes = Serialize.serialize(Response);
                                    socketChannel.write(ByteBuffer.wrap(bytes));
                                    //开始接收文件
                                    executorService.submit(()-> {
                                        try {handleContinueStorage(udpRcvSocket,path,storageOffset,rcvSize,totalTask,resId,offset);} catch (IOException e) {System.out.println("断点文件接收异常："+e);}
                                        System.out.println("\n等待客户端请求...");
                                    });

                                }else {
                                    byte[] bytes = Serialize.serialize(new ResponseMessage(AbstractMessage.ERROR));
                                    socketChannel.write(ByteBuffer.wrap(bytes));
                                }
                            }else {
                                byte[] bytes = Serialize.serialize(new ResponseMessage(AbstractMessage.ERROR));
                                socketChannel.write(ByteBuffer.wrap(bytes));
                            }
                            break;

                        //断点下载
                        case AbstractMessage.CONTINUE_DOWNLOAD:
                            System.out.println("处理请求：继续下载 | 客户端："+socketChannel.getRemoteAddress());
                            handleContinueDownload(socketChannel,msg,totalTask);
                            break;
                    }
                    //取消注册选择器和关闭通道,因为知道客户端请求后tcp连接就无用了
                    key.cancel();
                    socketChannel.close();
                }
                //移出键
                iterator.remove();
            }//遍历键
        }//while true
    }


    /**
     * 处理下载请求.首先获取客户端udp接收端口，然后发送
     * 包格式：4字节 seq + 8字节 CRC-32校验位 + 60K 数据
     * @create 2023/6/20 19:44
     **/
    private void handleDownMsg(InetAddress Ip,AbstractMessage msg) throws IOException, InterruptedException {
        LocationMessage msg1 = (LocationMessage) msg;
        totalTask.addAndGet(msg1.size);         //加到总任务量

        int port = msg1.udpPort;
        String path = msg1.filePath;
        Long offset = msg1.offset;
        Long size = msg1.size;
        //客户端地址
        InetSocketAddress clientUdpAddress = new InetSocketAddress(Ip, port);
        //发送文件
        UdpService udpService = new UdpService();
        udpService.sendFile(path,clientUdpAddress,offset,size,totalTask);
    }


    /**
     * 处理存储请求。首先返回服务的接收的udp端口，然后调用此函数开始接收。
     * 最后统计总共接收的字节数，然后向 meta 发送Id和 存储绝对路径
     * @create 2023/6/20 19:44
     **/
    private void handleStorageMsg(DatagramSocket socket,String fileName,long size,String resourceId) throws IOException {
        totalTask.addAndGet(size);                                      //加到总任务量

        FileState fileState = getFileStateForWrite();                   //获取非写状态的文件用于保存数据
        if (fileState == null) throw new RemoteException("无可写的文件！");

        //获取存储偏移
        String[] split = fileState.path.split(Pattern.quote(File.separator));
        int name = Integer.parseInt(split[split.length-1]);
        long offset = offsetMap.get(name).getOffset();
        //更新偏移量
        StringBuilder builder = new StringBuilder();
        FileOutputStream fos = new FileOutputStream("offset.record");
        offsetMap.get(name).setOffset(offset+size);
        Set<Integer> integers = offsetMap.keySet();
        Iterator<Integer> iterator = integers.iterator();
        while (iterator.hasNext()){
            Integer id = iterator.next();
            builder.append(offsetMap.get(id).toString());
        }
        //防止多线程同时写
        synchronized (lock){
            fos.write(builder.toString().getBytes());
        }
        fos.close();
        System.out.println("客户端文件将保存至："+fileState.path+"  offset: "+offset+"  size: "+size);
        //接收文件并获取客户端udp地址
        UdpService udpService = new UdpService();
        long rcvSize = udpService.receiveFile(socket, fileState.path, size, totalTask);
        //接收未完时存储断点
        if (rcvSize < size){
            BreakStorage breakStorage = new BreakStorage(resourceId, Id, fileState.path, rcvSize);
            socketToMeta.getOutputStream().write(Objects.requireNonNull(Serialize.serialize(breakStorage)));
        }
        fileState.writing = false;

        //向 meta 发送存储信息
        StorageInfo storageInfo = new StorageInfo(resourceId,Id, fileName,fileState.path, offset, size);
        socketToMeta.getOutputStream().write(Objects.requireNonNull(Serialize.serialize(storageInfo)));
    }

    private void handleContinueStorage(DatagramSocket socket,String path,Long offset,Long size,AtomicLong totalTask,String resId,Long clientOffset) throws IOException {
        //增加总任务量
        totalTask.addAndGet(size);
        UdpService udpService = new UdpService();
        Long rcvSize = udpService.receiveFileTest(socket, path, offset, size, totalTask);
        //传输完毕，删除断点记录
        if (Objects.equals(rcvSize, size)){
            BreakDel breakDel = new BreakDel(resId);
            socketToMeta.getOutputStream().write(Objects.requireNonNull(Serialize.serialize(breakDel)));
        }
        //更新断点偏移
        else {
            BreakStorage breakStorage = new BreakStorage(resId, Id, path, clientOffset + rcvSize);
            socketToMeta.getOutputStream().write(Objects.requireNonNull(Serialize.serialize(breakStorage)));
        }
    }

    private void handleContinueDownload(SocketChannel socketChannel,AbstractMessage msg,AtomicLong totalTask) throws IOException, InterruptedException {
        BreakDownload breakDownload = (BreakDownload) msg;
        //检查连接
        //查询资源
        byte[] bytes = Serialize.serialize(new DownMessage(breakDownload.resourceId));
        socketToMeta.getOutputStream().write(bytes);
        AbstractMessage response = Serialize.deserialize(socketToMeta.getInputStream());
        //存在该资源
        boolean isPass = false;
        assert response != null;
        if (response.code == AbstractMessage.OK){
            LocationMessage response1 = (LocationMessage) response;
            //是本机
            if(Id.equals(response1.nodeId)){
                //是否未存完
                socketToMeta.getOutputStream().write(Objects.requireNonNull(Serialize.serialize(new BreakQuery(breakDownload.resourceId))));
                AbstractMessage queryBreakRes = Serialize.deserialize(socketToMeta.getInputStream());
                //已存完
                assert queryBreakRes != null;
                if (queryBreakRes.code == AbstractMessage.ERROR){
                    String filePath = response1.filePath;
                    Long offset = response1.offset;         //文件存储偏移量
                    //保证续下载的起点不超过文件本身尺寸
                    if (breakDownload.offset < response1.size){
                        totalTask.addAndGet(response1.size - breakDownload.offset);     //增加任务量
                        isPass = true;
                        socketChannel.write(ByteBuffer.wrap(Objects.requireNonNull(Serialize.serialize(new ResponseMessage(AbstractMessage.OK)))));
                        //开始发送文件
                        UdpService udpService = new UdpService();
                        InetAddress ip = ((InetSocketAddress)socketChannel.getRemoteAddress()).getAddress();
                        InetSocketAddress address = new InetSocketAddress(ip, breakDownload.udpPort);
                        udpService.sendFile(filePath,address,offset + breakDownload.offset,response1.size - breakDownload.offset,totalTask);

                    }
                }
            }
        }
        //未通过检查时
        if (!isPass){
            ResponseMessage errorRes = new ResponseMessage(AbstractMessage.ERROR);
            socketChannel.write(ByteBuffer.wrap(Objects.requireNonNull(Serialize.serialize(errorRes))));
        }
    }
    /**
     * 定时发送状态心跳包
     * @create 2023/6/20 12:32
     **/
    private void scheduleSendState(int time, TimeUnit unit) {
         scheduledStateSend.scheduleWithFixedDelay(() -> {
            //读取空间
            File file = new File(System.getProperty("user.dir"));
            long freeSpace = file.getFreeSpace();
            //发送状态心跳包
            DataNodeStateMessage msg = new DataNodeStateMessage(Id, freeSpace * 1.0 / 1024 / 1024 / 1024, totalTask.get());
            try {
                socketToDis.getOutputStream().write(Serialize.serialize(msg));
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("心跳发送异常：" + e);
            } catch (Exception e) {
                System.out.println("心跳发送异常：" + e);
            }
        }, 0, time, unit);
    }

    public static boolean checkConnect(Socket socket, Reconnect recon) throws InterruptedException {
        try{
            if (recon==null) throw new NullPointerException("recon == null，未实现重连逻辑！");
            socket.getOutputStream().write(Objects.requireNonNull(Serialize.serialize(new OnlineDectect())));
        }catch (NullPointerException e){
            System.out.println(e.getMessage());
        } catch (Exception e) {
            for (int i=1;i<7;i++){
                System.out.println("断线重连中...第 "+i+" 次尝试");
                boolean result = recon.reconnect();
                if (result){
                    System.out.println("重连成功！");
                    return true;
                }
                Thread.sleep(2500);
            }
            return false;
        }
        return true;
    }
}