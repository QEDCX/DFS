package message.client;

import message.AbstractMessage;

public class StorageMessage extends AbstractMessage {
    public String fileName;
    public Long size;
    public StorageMessage(String fileName,Long size) {
        code = STORAGE;
        this.fileName = fileName;
        this.size = size;
    }
}
