package message.datanode;

import message.AbstractMessage;

/**
 * data node 向 meta 发送的存储信息
 *
 * @return
 * @create 2023/6/22 23:04
 **/
public class StorageInfo extends AbstractMessage {
    public String resourceId;
    public Integer id;
    public String fileName;
    public String filePath;
    public Long offset;
    public Long size;

    public StorageInfo(String resourceId,Integer id,String fileName, String filePath, Long offset, Long size) {
        code = AbstractMessage.STORAGE_INFO;
        this.resourceId = resourceId;
        this.id = id;
        this.fileName = fileName;
        this.filePath = filePath;
        this.offset = offset;
        this.size = size;
    }
}
