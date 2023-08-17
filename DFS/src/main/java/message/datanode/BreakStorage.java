package message.datanode;

import message.AbstractMessage;

public class BreakStorage extends AbstractMessage {
    public String resourceId;
    public Integer nodeId;
    public String path;
    public Long offset;

    public BreakStorage(String resourceId,Integer nodeId, String path, Long offset) {
        this.code = Break_Storage;
        this.resourceId = resourceId;
        this.nodeId = nodeId;
        this.path = path;
        this.offset = offset;
    }
}
