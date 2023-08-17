package message;

import java.net.InetSocketAddress;

/**
 *
 * @return 当 data node 接收，包含了分配的 id （首次分配）和 name node 端点，其余的接收代表 OK
 * @create 2023/6/19 13:07
 **/
public class ResponseMessage extends AbstractMessage {
    public Integer allocatedDataNodeId;
    public InetSocketAddress nameNodeEndPoint;

    public ResponseMessage(int code,InetSocketAddress nameNodeEndPoint,Integer allocatedDataNodeId) {
        this.nameNodeEndPoint = nameNodeEndPoint;
        this.allocatedDataNodeId = allocatedDataNodeId;
        this.code = code;
    }
    
    public ResponseMessage(int code) {
        this.code = code;
    }
}
