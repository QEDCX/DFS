package message.datanode;

import message.AbstractMessage;

/**
 * 由data node生成，返回给客户端的资源id
 *
 * @return
 * @create 2023/6/23 10:27
 **/
public class ResourceId extends AbstractMessage {
    public String resourceId;

    public ResourceId(String resourceId) {
        this.resourceId = resourceId;
    }
}
