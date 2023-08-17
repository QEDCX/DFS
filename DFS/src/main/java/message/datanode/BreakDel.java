package message.datanode;

import message.AbstractMessage;

public class BreakDel extends AbstractMessage {
    public String resId;

    public BreakDel(String resId) {
        this.code = Break_DELETE;
        this.resId = resId;
    }
}
