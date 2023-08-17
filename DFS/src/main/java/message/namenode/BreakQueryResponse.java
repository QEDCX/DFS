package message.namenode;

import message.AbstractMessage;

public class BreakQueryResponse extends AbstractMessage {
    public Integer nodeId;
    public String path;
    public Long offset;

    public BreakQueryResponse(Integer nodeId, String path, Long offset) {
        this.code = Break_QUERY_Response;
        this.nodeId = nodeId;
        this.path = path;
        this.offset = offset;
    }
}
