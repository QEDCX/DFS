package dispatcher;

import message.AbstractMessage;
import message.LocationMessage;
import message.OnlineDectect;
import message.ResponseMessage;
import message.client.DownMessage;
import message.datanode.DataNodeStateMessage;
import message.datanode.DataRegisterMessage;
import message.MetaAddressResponse;
import message.namenode.NameRegisterMessage;
import util.Serialize;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Dispatcher{
    private int id;
    private int id1;
    private final HashMap<Integer,DataNode> readMap = new HashMap<>();
    private final HashMap<Integer,DataNode> writeMap = new HashMap<>();
    private final HashMap<Integer,Socket> metaMap = new HashMap<>();        //和元数据节点通信的套接字
    private final HashMap<Integer,InetSocketAddress> metaAddressMap = new HashMap<>();      //元数据节点监听的地址
    private final static double minSpace = 128;
    private ServerSocket serverSocket;
    private final ExecutorService executorService = Executors.newFixedThreadPool(50);
    private final ScheduledExecutorService onlineCheckExecutor = Executors.newScheduledThreadPool(1);
    public void app_run(int listenPort) {
        try {
            ini(listenPort);
            scheduleCheckOnline(5, TimeUnit.SECONDS);
            System.out.println("开始处理请求...");
            while (true) {
                Socket client = serverSocket.accept();
                executorService.submit(new Handler(client));
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }



    /**
     * 定时检查数据节点是否在线
     * @create 2023/6/20 10:00
     **/
    private void scheduleCheckOnline(int time,TimeUnit unit){
        onlineCheckExecutor.scheduleWithFixedDelay(()->{
            //检查元数据节点
            try {
                Socket socket = metaMap.get(id1-1);
                if (socket != null){
                    socket.getOutputStream().write(Objects.requireNonNull(Serialize.serialize(new OnlineDectect())));
                }
            }catch (Exception e){
                metaMap.remove(id1-1);
                metaAddressMap.remove(id1-1);
                System.out.println("元数据节点下线："+--id1);
            }
            //检查数据节点
            Iterator<DataNode> iterator = readMap.values().iterator();
            while(iterator.hasNext()){
                DataNode next = iterator.next();
                if (next.isOnline){
                    next.isOnline = false;
                }else {
                    try {
                        next.socket.close();
                    } catch (IOException e) {
                        System.out.println("定时任务异常"+e);
                    }
                    readMap.remove(next.id);
                    writeMap.remove(next.id);
                    System.out.println("数据节点下线："+next.id);
                }
            }
        },0,time,unit);
    }
    /**
     * 生成数据节点的ID,生成后要更新文件
     * @return 生成的id
     * @create 2023/6/18 19:47
     **/
    private int genId() throws IOException {
        id += 1;
        FileOutputStream fos = new FileOutputStream("maxId.txt");
        fos.write((id+"\n").getBytes());
        fos.close();
        return id;
    }

    /**
     * 读取已分配过的最大id，以保证分配的Id不重复，并保证至少注册一个 name node 和 data node 后才开始工作
     * BUG记录：必须先让 name node 注册，否则对 data node 的响应中没有 name node 的地址
     * BUG修正：将到达的 data node 注册请求缓存，待元数据注册后再注册数据节点
     * BUG记录：集合有修改时需要迭代器，否则出现并发修改异常
     * @return 
     * @create 2023/6/18 10:21
     **/
    private void ini(int port) throws IOException {
       File file = new File("maxId.txt");
       if (file.exists()) {
           //读取已分配的最大Id
           FileInputStream fis = new FileInputStream("maxId.txt");
           Scanner scanner = new Scanner(fis);
           id = Integer.parseInt(scanner.nextLine());
           fis.close();
       }
       else {
           file.createNewFile();
           id = 9999;   //id从10000开始分配
       }
        //当data node先请求时，用于缓存data node的注册
        class Cache{
            final AbstractMessage ob;
            final Socket socket;
            public Cache(AbstractMessage ob, Socket socket) {
                this.ob = ob;
                this.socket = socket;
            }
        }
        List<Cache> caches = new ArrayList<>();

       serverSocket = new ServerSocket(port);
       System.out.println("初始化中...\n"+"Dispatcher已绑定端口："+port);
       //先注册name node，且至少注册一个，否则对data node的响应中没有 name node 的地址
       System.out.print("等待元数据节点注册: ");
       while (metaMap.size() == 0){
           Socket accept = serverSocket.accept();
           //反序列化消息
           InputStream inputStream = accept.getInputStream();
           AbstractMessage ob = Serialize.deserialize(inputStream);
           if (ob.code == AbstractMessage.NAME_NODE_REGISTER){
               //保存通信套接字、监听地址并响应
               registerMetaNode(accept,ob);
               accept.setKeepAlive(true);
           }else if (ob.code == AbstractMessage.DATA_NODE_REGISTER){
               caches.add(new Cache(ob,accept));
           }else {
               System.out.println("异常连接，已关闭");
               inputStream.close();
               accept.close();
           }
       }
       System.out.print("已注册\n等待数据节点注册: ");
       //从缓存中取请求，BUG记录：因为集合有删除操作，只能用迭代器，否则出现并发修改异常
       Iterator<Cache> iterator = caches.iterator();
       while(iterator.hasNext()){
           Cache cache = iterator.next();
           registerDataNode(cache.socket,cache.ob);
           executorService.submit(new Handler(cache.socket));   //线程处理请求
           cache.socket.setKeepAlive(true);
           iterator.remove();
       }
        //至少注册一个 data node
       while(readMap.size() == 0){
           Socket accept = serverSocket.accept();
           //反序列化消息
           InputStream inputStream = accept.getInputStream();
           AbstractMessage ob = Serialize.deserialize(inputStream);
           //数据节点注册
           if (ob.code == AbstractMessage.DATA_NODE_REGISTER){
               //注册并响应
               registerDataNode(accept,ob);
               accept.setKeepAlive(true);
               //线程处理请求
               executorService.submit(new Handler(accept));
           } else {
               System.out.println("异常连接，已关闭");
               inputStream.close();
               accept.close();
           }
       }
        System.out.println("已注册");
        System.out.println("初始化完成\n");
    }


    /**
     * 返回文件位置信息给客户端
     * @return LocationMessage 或 null
     * @create 2023/6/18 17:34
     **/
    private LocationMessage getLocationInfo(AbstractMessage msg) throws IOException {
        LocationMessage locationMessage = null;
        //从名称节点取数据所在的节点
        DownMessage downMsg = (DownMessage) msg;
        //询问name node
        Collection<Socket> metaSockets = metaMap.values();
        for (Socket socket:metaSockets){
            socket.getOutputStream().write(Objects.requireNonNull(Serialize.serialize(downMsg)));
            socket.getOutputStream().flush();

            InputStream inputStream = socket.getInputStream();
            LocationMessage res = (LocationMessage) Serialize.deserialize(inputStream);
            if (res.code == AbstractMessage.OK){
                locationMessage = res;
                break;
            }
        }
        //根据 meta 返回的id,填充数据节点的监听套接字
        locationMessage.nodeEndPoint = readMap.get(locationMessage.nodeId).getListenSocket();
        return locationMessage;
    }


    /**
     * 返回处理存储请求的节点给客户端
     * @return DataNodeInfoMessage（保存了节点的端点信息）
     * @create 2023/6/18 22:02
     **/
    private LocationMessage getStorageNode(){
        DataNode node;
        Iterator<Integer> iterator = writeMap.keySet().iterator();
        int tmp = iterator.next();
        node = writeMap.get(tmp);
        while (iterator.hasNext()){
            tmp = iterator.next();
            DataNode dtmp = writeMap.get(tmp);
            if (dtmp.totalTask < node.totalTask){
                node = dtmp;
            }
        }
        //返回客户端监听套接字地址
        InetSocketAddress socketAddress = new InetSocketAddress(node.socket.getInetAddress(), node.listenPort);
        return new LocationMessage(node.id,socketAddress,null,null,null,null);
    }


    /**
     * 处理 data node 的心跳包 ，用于更新节点的任务量、剩余空间和在线状态
     * @return
     * @create 2023/6/19 12:47
     **/
    private void handleNodeState(AbstractMessage abstractMessage){
        DataNodeStateMessage msg = (DataNodeStateMessage) abstractMessage;
        //获取node对象
        DataNode node = readMap.get(msg.nodeId);
        System.out.printf("nodeId: %5d   磁盘容量: %.2f GB   任务量: %.2f MB\n",msg.nodeId,msg.space,msg.totalTask*1.0 / 1024 / 1024);
        //更新任务量和容量
        node.space = msg.space;     //以GB为单位
        node.totalTask = msg.totalTask;
        //判断是否能继续存储,当前认为是128MB
        if (node.space * 1024 < minSpace){
            writeMap.remove(msg.nodeId);
        }

        //设置心跳标志位
        node.isOnline = true;
    }

    /**
     * 注册data node，并返回响应
     * @create 2023/6/18 10:22
     **/
    private void registerDataNode(Socket nodeSocket,AbstractMessage ob) throws IOException {
        DataRegisterMessage ob1 = (DataRegisterMessage)ob;
        //获取节点的Id,如果是第一次注册则生成并同时返回id
        Integer nodeId;
        if (ob1.isAllocateId){
            nodeId = genId();
        } else{
            nodeId = ob1.Id;
        }
        //响应
        nodeSocket.getOutputStream().write(Objects.requireNonNull(Serialize.serialize(new ResponseMessage(AbstractMessage.OK, metaAddressMap.get(0), nodeId))));

        //注册
        DataNode node = new DataNode(nodeId, nodeSocket, ob1.listenPort);
        node.isOnline = true;
        readMap.put(nodeId,node);
        writeMap.put(nodeId,node);
    }
    
    /**
     * 注册name node,并返回响应
     * @create 2023/6/18 10:22
     **/
    private void registerMetaNode(Socket nameSocket,AbstractMessage ob) throws IOException {
        metaMap.put(id1,nameSocket);
        int lisPort = ((NameRegisterMessage)ob).listenPort;
        metaAddressMap.put(id1,new InetSocketAddress(nameSocket.getInetAddress(),lisPort));
        id1 += 1;
        nameSocket.getOutputStream().write(Objects.requireNonNull(Serialize.serialize(new ResponseMessage(AbstractMessage.OK))));
    }
    

     /**
      * 打印 name node 的数目
      * @create 2023/6/19 12:45
      **/
    private void printMetaNodes(){
        System.out.println("\n元数据节点："+metaMap.size());
        for (Socket socket:metaMap.values()){
            System.out.println("地址: "+socket.getRemoteSocketAddress());
        }
        System.out.println("\n");
    }

    /**
     * 打印 data node 的信息
     * @create 2023/6/19 12:44
     **/
    private void printDataNodes(){
        System.out.println("\n数据节点："+readMap.size());
        for (DataNode node: readMap.values()){
            System.out.println(node);
        }
        System.out.println("\n");
    }


    static class DataNode{
        int id;
        String name;
        String des;
        Socket socket;
        int listenPort;
        double space;
        long totalTask;
        boolean isOnline;
        public DataNode(int id, Socket socket,int listenPort) {
            this.id = id;
            this.socket = socket;
            this.name = ((InetSocketAddress)socket.getRemoteSocketAddress()).getHostName();
            this.des = "none";
            this.listenPort = listenPort;
        }

        @Override
        public String toString() {
            return "ID: "+id+"  名称: "+name+"  容量: "+space+"  GB  任务量: "+totalTask*1.0f/1024+" MB  描述: "+des+"  地址: "+socket.getRemoteSocketAddress();
        }

        public InetSocketAddress getListenSocket(){
            return new InetSocketAddress(socket.getInetAddress(),listenPort);
        }
    }
    class Handler implements Runnable{
        Socket socket;

        public Handler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
           try {
                 boolean exit = false;
                 InputStream inputStream = socket.getInputStream();
                 OutputStream outputStream = socket.getOutputStream();
                 while(true){
                     //是否退出
                     if (exit){
                         inputStream.close();
                         outputStream.close();
                         socket.close();
                         System.out.println("已退出线程");
                         break;
                     }
                    //反序列化消息
                    AbstractMessage msg = Serialize.deserialize(inputStream);
                    if (msg !=null){
                        switch (msg.code){
                            //返回位置信息并关闭连接
                             case AbstractMessage.DOWN:
                                 System.out.print("分配下载任务节点：");
                                 LocationMessage locationMessage = getLocationInfo(msg);     //查询并返回存储数据的节点
                                 outputStream.write(Objects.requireNonNull(Serialize.serialize(locationMessage)));
                                 System.out.println(locationMessage.nodeId);
                                 exit = true;
                                 break;
                             //返回任务节点并关闭连接
                             case AbstractMessage.STORAGE:
                                 System.out.print("分配存储任务节点：");
                                 LocationMessage storageNode = getStorageNode();             //返回任务量最小的可写节点
                                 outputStream.write(Objects.requireNonNull(Serialize.serialize(storageNode)));
                                 System.out.println(storageNode.nodeId);
                                 exit = true;
                                 break;
                             //注册 data node
                             case AbstractMessage.DATA_NODE_REGISTER:
                                 registerDataNode(socket,msg);
                                 break;
                             //处理心跳包
                             case AbstractMessage.DATA_NODE_STATE:
                                 handleNodeState(msg);
                                 break;
                             //注册 meta node
                             case AbstractMessage.NAME_NODE_REGISTER:
                                 registerMetaNode(socket,msg);
                                 break;
                             //查询元数据节点地址用于断线重连
                            case AbstractMessage.QUERY_META_ADDRESS:
                                //检查元数据节点
                                boolean isOnline = true;
                                try {
                                    Socket socket = metaMap.get(id1-1);
                                    if (socket != null){
                                        socket.getOutputStream().write(Objects.requireNonNull(Serialize.serialize(new OnlineDectect())));
                                    }
                                }catch (Exception e){
                                    metaMap.remove(id1-1);
                                    metaAddressMap.remove(id1-1);
                                    isOnline = false;
                                    System.out.println("元数据节点下线："+--id1);
                                }
                                if (isOnline){
                                    MetaAddressResponse metaAddrRes;
                                    Iterator<InetSocketAddress> iterator = metaAddressMap.values().iterator();
                                    if (iterator.hasNext()){
                                        metaAddrRes = new MetaAddressResponse(iterator.next());
                                        byte[] bytes = Serialize.serialize(metaAddrRes);
                                        outputStream.write(bytes);
                                    }else {
                                        byte[] bytes = Serialize.serialize(new ResponseMessage(AbstractMessage.ERROR));
                                        outputStream.write(bytes);
                                    }
                                } else {
                                    byte[] bytes = Serialize.serialize(new ResponseMessage(AbstractMessage.ERROR));
                                    outputStream.write(bytes);
                                }
                                break;
                        }
                    }
                    else {exit = true;System.out.print("主机断开连接，即将退出本线程：");}
                 }
           } catch (Exception e){
               System.out.println("线程异常: "+e);
           }
        }
    }
}
