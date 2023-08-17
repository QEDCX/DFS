package message.client;

import message.AbstractMessage;

public class DownMessage extends AbstractMessage {
    public String ResourceId;
    public DownMessage(String resId) {
        code = DOWN;
        ResourceId = resId;
    }
}
