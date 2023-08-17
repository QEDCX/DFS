package message.client;

import message.AbstractMessage;

public class StorageBreak extends AbstractMessage {
    public String resId;
    public StorageBreak(String resId) {
        code = CONTINUE_STORAGE;
        this.resId = resId;
    }
}
