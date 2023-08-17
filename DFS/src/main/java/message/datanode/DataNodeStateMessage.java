package message.datanode;

import message.AbstractMessage;

/**
 * 心跳包
 * @create 2023/6/22 23:08
 **/
public class DataNodeStateMessage extends AbstractMessage {
    public int nodeId;
    public double space;      //GB为单位
    public Long totalTask;    //字节为单位
    public DataNodeStateMessage(int id,double space,Long totalTask) {
        code = AbstractMessage.DATA_NODE_STATE;
        this.nodeId = id;
        this.space = space;
        this.totalTask = totalTask;
    }
}
