package Meta;

import message.AbstractMessage;
import message.LocationMessage;
import message.ResponseMessage;
import message.client.DownMessage;
import message.datanode.BreakDel;
import message.datanode.BreakQuery;
import message.datanode.BreakStorage;
import message.datanode.StorageInfo;
import message.namenode.*;
import redis.clients.jedis.Jedis;
import util.Serialize;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

public class MetaData {
    private int listenPort;
    Jedis jedis;
    SocketChannel socketToDisChannel;

    private void insertStorageInfo(StorageInfo info){
        jedis.select(0);
        String resourceId = info.resourceId;
        String nodeId = String.valueOf(info.id);
        String fileName = info.fileName;
        String filePath = info.filePath;
        String offset = String.valueOf(info.offset);
        String size = String.valueOf(info.size);
        jedis.hset(resourceId,"resourceId",resourceId);
        jedis.hset(resourceId,"nodeId",nodeId);
        jedis.hset(resourceId,"fileName",fileName);
        jedis.hset(resourceId,"filePath",filePath);
        jedis.hset(resourceId,"offset",offset);
        jedis.hset(resourceId,"size",size);
    }

    private LocationMessage getStorageInfo(String reId){
        jedis.select(0);
        Map<String, String> map = jedis.hgetAll(reId);
        if(map.isEmpty()) return null;
        Integer nodeId = Integer.parseInt(map.get("nodeId"));
        String fileName = map.get("fileName");
        String filePath = map.get("filePath");
        long offset = Long.parseLong(map.get("offset"));                                     //将字符串转换为long
        long size = Long.parseLong(map.get("size"));
        return new LocationMessage(nodeId,null,fileName,filePath,offset,size);   //客户端地址由dis根据id赋值
    }

    private void addBreak(BreakStorage msg){
        jedis.select(1);
        String resourceId = msg.resourceId;
        Integer nodeId = msg.nodeId;
        String path = msg.path;
        Long offset = msg.offset;
        jedis.hset(resourceId,"nodeId", String.valueOf(nodeId));
        jedis.hset(resourceId,"path",path);
        jedis.hset(resourceId,"offset", String.valueOf(offset));
    }


    private void deleteBreak(BreakDel msg){
        jedis.select(1);
        jedis.del(msg.resId);
    }

    private BreakQueryResponse queryBreak(BreakQuery msg){
        jedis.select(1);
        Map<String, String> map = jedis.hgetAll(msg.resId);
        if (map.isEmpty()) return null;
        Integer nodeId = Integer.valueOf(map.get("nodeId"));
        String path = map.get("path");
        Long offset = Long.valueOf(map.get("offset"));
        return new BreakQueryResponse(nodeId,path,offset);
    }

    public void app_run(int listenPort,String remoteIp,int remotePort,String redisIp) {
        try {
            this.listenPort = listenPort;
            ini(remoteIp, remotePort,redisIp);
            handler();
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 连接redis、注册到 dis
     *
     * @create 2023/6/23 13:25
     **/
    private void ini(String remoteIp,int remotePort,String redisIp) throws IOException {
        //连接redis
        jedis = new Jedis(redisIp,6379);
        jedis.select(0);

        //注册 Dis
        System.out.print("注册到Dispatcher: ");
        socketToDisChannel = SocketChannel.open(new InetSocketAddress(remoteIp,remotePort));
        NameRegisterMessage registerMessage = new NameRegisterMessage(listenPort);
        socketToDisChannel.socket().setReceiveBufferSize(2048);
        socketToDisChannel.socket().getOutputStream().write(Objects.requireNonNull(Serialize.serialize(registerMessage)));
        //接收响应
        AbstractMessage res = Serialize.deserialize(socketToDisChannel.socket().getInputStream());
        assert res != null;
        if (res.code == AbstractMessage.OK){
            System.out.println("注册成功\n");
        }
    }

    
    /**
     * 处理
     *
     * @create 2023/6/23 13:35
     **/
    private void handler() throws IOException {
        System.out.println("等待请求...\n");
        Selector selector = Selector.open();
        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.bind(new InetSocketAddress("localhost",listenPort));
        //注册到选择器
        ssc.configureBlocking(false);
        ssc.register(selector, SelectionKey.OP_ACCEPT);

        //和dis连接的socket注册到选择器
        socketToDisChannel.configureBlocking(false);
        socketToDisChannel.register(selector,SelectionKey.OP_READ);
        while (true){
            selector.select();
            //返回迭代器
            Iterator<SelectionKey> iterator  = selector.selectedKeys().iterator();
            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();
                //数据节点连接,注册到选择器
                if (key.isAcceptable()) {
                    SocketChannel socketChannel = ssc.accept();
                    socketChannel.socket().setSendBufferSize(2048);
                    System.out.println("请求信息：注册 | 数据节点："+socketChannel.getRemoteAddress());
                    socketChannel.configureBlocking(false);
                    socketChannel.register(selector, SelectionKey.OP_READ);
                    //响应
                    ByteBuffer buffer = ByteBuffer.allocate(1024);
                    buffer.put(Objects.requireNonNull(Serialize.serialize(new ResponseMessage(AbstractMessage.OK))));
                    buffer.flip();
                    socketChannel.write(buffer);
                }
                //读取数据节点或dis的存储、查询信息
                else if (key.isReadable()) {
                    ByteBuffer buffer = ByteBuffer.allocate(1024);
                    byte[] msgBytes;            //存储消息的字节数组
                    SocketChannel socketChannel = (SocketChannel) key.channel();

                    //反序列化消息
                    try{socketChannel.read(buffer);}
                    catch (SocketException e){key.cancel();socketChannel.close();System.out.println("异常：和主机断开连接 | e: "+e);continue;}
                    catch (Exception e){key.cancel();socketChannel.close();System.out.println("异常："+e);continue;}
                    buffer.flip();
                    msgBytes = new byte[buffer.remaining()];                 //分配空间
                    buffer.get(msgBytes,0, msgBytes.length);           //填充数据
                    AbstractMessage msg = Serialize.deserialize(msgBytes);   //反序列化
                    if (msg != null) {
                        switch (msg.code) {
                            //dis获取存储信息
                            case AbstractMessage.DOWN:
                                System.out.println("请求信息：获取元数据");
                                //获取存储信息
                                String reId = ((DownMessage) msg).ResourceId;
                                LocationMessage locationMessage = getStorageInfo(reId);
                                if (locationMessage != null) {
                                    //返回信息
                                    byte[] serialize = Serialize.serialize(locationMessage);
                                    buffer.clear();
                                    buffer.put(serialize);
                                    buffer.flip();
                                    while (buffer.hasRemaining()) {
                                        socketChannel.write(buffer);
                                    }
                                } else {
                                    byte[] serialize = Serialize.serialize(new ResponseMessage(AbstractMessage.ERROR));
                                    socketChannel.write(ByteBuffer.wrap(serialize));
                                }
                                break;
                            //数据节点存储信息
                            case AbstractMessage.STORAGE_INFO:
                                System.out.println("请求信息：存储元数据");
                                insertStorageInfo((StorageInfo) msg);
                                break;
                            //断点存储
                            case AbstractMessage.Break_Storage:
                                System.out.println("请求信息：断点存储");
                                addBreak((BreakStorage) msg);
                                break;
                            //断点删除
                            case AbstractMessage.Break_DELETE:
                                System.out.println("请求信息：断点删除");
                                deleteBreak((BreakDel)msg);
                                break;
                            //断点查询
                            case AbstractMessage.Break_QUERY:
                                System.out.println("请求信息：查询断点");
                                BreakQueryResponse breakQueryResponse = queryBreak((BreakQuery) msg);
                                if (breakQueryResponse == null){
                                    byte[] bytes = Serialize.serialize(new ResponseMessage(AbstractMessage.ERROR));
                                    assert bytes != null;
                                    socketChannel.write(ByteBuffer.wrap(bytes));
                                }else {
                                    byte[] bytes = Serialize.serialize(breakQueryResponse);
                                    assert bytes != null;
                                    socketChannel.write(ByteBuffer.wrap(bytes));
                                }
                                break;
                            //DIS心跳探测包
                            case AbstractMessage.ONLINE_DECTECT:
                                //什么也不做
                                break;
                            default:
                                System.out.println("异常请求");
                        }
                    }else {
                        System.out.println("msg为null，本次请求不处理");
                    }
                }//if can read
                iterator.remove();
            }//遍历键
        }//while true
    }
}
