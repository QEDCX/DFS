package message.client;

import message.AbstractMessage;

public class BreakDownload extends AbstractMessage {
    public String resourceId;
    public long offset;
    public int udpPort;


    public BreakDownload(String resourceId, long offset,int udpPort) {
        code = AbstractMessage.CONTINUE_DOWNLOAD;
        this.udpPort = udpPort;
        this.resourceId = resourceId;
        this.offset = offset;
    }
}
