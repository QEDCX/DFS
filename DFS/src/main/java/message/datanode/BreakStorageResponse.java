package message.datanode;

import message.AbstractMessage;

public class BreakStorageResponse extends AbstractMessage {
    public long offset;
    public int rcvPort;
    public BreakStorageResponse(long offset,int rcvPort) {
        this.code = Break_Storage_Response;
        this.offset = offset;
        this.rcvPort = rcvPort;
    }
}
