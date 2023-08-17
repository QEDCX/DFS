package client.entity;

import java.io.Serializable;
/*
* 服务端先返回resId
* 客户端将资源id、服务端地址一起保存到断点文件
* 请求时存储时先检查断点文件，若是断点则附带resId
* 服务端接受到id后向元数据服务器查询
* */
public class BreakRecord implements Serializable {
    String path;
    String nodeIp;
    String nodePort;
    String resourceId;
    public BreakRecord() {
    }

    public BreakRecord(String path, String nodeIp, String nodePort, String resourceId) {
        this.path = path;
        this.nodeIp = nodeIp;
        this.nodePort = nodePort;
        this.resourceId = resourceId;
    }

    @Override
    public String toString() {
        return  path + "——" + nodeIp + "——" + nodePort + "——"+resourceId+"\n";
    }

    public String getPath() {
        return path;
    }

    public String getResourceId() {
        return resourceId;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getNodeIp() {
        return nodeIp;
    }

    public void setNodeIp(String nodeIp) {
        this.nodeIp = nodeIp;
    }

    public String getNodePort() {
        return nodePort;
    }

    public void setNodePort(String nodePort) {
        this.nodePort = nodePort;
    }
}





















































