package client.entity;

public class DownBreakRecord {
    public String resourceId;
    public long totalSize;
    public String ip;
    public int port;
    public String storagePath;

    public String getResourceId() {
        return resourceId;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    public long getTotalSize() {
        return totalSize;
    }

    public void setTotalSize(long totalSize) {
        this.totalSize = totalSize;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getStoragePath() {
        return storagePath;
    }

    public void setStoragePath(String storagePath) {
        this.storagePath = storagePath;
    }

    public DownBreakRecord(String resourceId, String ip, int port, long totalSize, String storagePath) {
        this.resourceId = resourceId;
        this.ip = ip;
        this.port = port;
        this.totalSize = totalSize;
        this.storagePath = storagePath;
    }

    @Override
    public String toString() {
        return resourceId+"——"+ip+"——"+port+"——"+totalSize+"——"+storagePath+"\n";
    }
}
