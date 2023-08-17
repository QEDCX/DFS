package message.datanode;

import message.AbstractMessage;

public class BreakQuery extends AbstractMessage {
    public String resId;

    public BreakQuery(String resId) {
        this.code = Break_QUERY;
        this.resId = resId;
    }
}
