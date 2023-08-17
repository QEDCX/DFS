package message.datanode;

import message.AbstractMessage;


/**
 * 传文件前先发送接收方的udp接收端口
 *
 * @return
 * @create 2023/6/21 22:47
 **/
public class ServerUdpRcvPort extends AbstractMessage {
    public int port;
}
