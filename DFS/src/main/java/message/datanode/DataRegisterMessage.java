package message.datanode;

import message.AbstractMessage;

public class DataRegisterMessage extends AbstractMessage {
    public boolean isAllocateId;
    public Integer Id;
    public Integer listenPort;

    public DataRegisterMessage(boolean isAllocateId,Integer id,Integer listenPort) {
        this.isAllocateId = isAllocateId;
        this.Id = id;
        code = DATA_NODE_REGISTER;
        this.listenPort = listenPort;
    }

}