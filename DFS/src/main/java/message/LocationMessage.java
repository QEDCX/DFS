package message;

import java.net.InetSocketAddress;

/**
 * 下载请求中返回代表存储的 data node 位置 及文件信息
 * 客户端该信息将发给 data node 处理
 * resourceId:
 *         filename:
 *         nodeId:
 *         filepath
 *         offset
 *         size
 *
 * @return
 * @create 2023/6/19 12:14
 **/

public class LocationMessage extends AbstractMessage {
    public Integer nodeId;
    public InetSocketAddress nodeEndPoint;
    public String fileName;
    public String filePath;
    public Long offset;
    public Long size;
    public Integer udpPort; //客户端下载文件的udp接收端口

    //供name node 调用，返回文件位置信息
    public LocationMessage(Integer nodeId,InetSocketAddress nodeEndPoint,String fileName,String path,Long offset,Long size) {
        this.code = AbstractMessage.OK;
        this.nodeId = nodeId;
        this.nodeEndPoint = nodeEndPoint;
        this.fileName = fileName;
        this.filePath = path;
        this.offset = offset;
        this.size = size;
    }

    //供客户端下载时调用，设置udp接收端口和更改code为DOWN
    public LocationMessage setLocationMessageToDataNode(int rcvPort){
        this.code = AbstractMessage.DOWN;
        this.udpPort = rcvPort;
        return this;
    }


    @Override
    public String toString() {
        return "数据节点: "+nodeEndPoint+"  文件名: "+fileName+"  存储路径: "+filePath+"  偏移量: "+offset+"  大小: "+size;
    }
}
