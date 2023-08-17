package message.datanode;

import message.AbstractMessage;

public class QueryMetaAddress extends AbstractMessage {
    public QueryMetaAddress() {
        code = QUERY_META_ADDRESS;
    }
}
