package client.entity;

public class OffsetRecord {
    Integer name;
    Long offset;

    public OffsetRecord(Integer name, Long offset) {
        this.name = name;
        this.offset = offset;
    }

    public Integer getName() {
        return name;
    }

    public Long getOffset() {
        return offset;
    }

    public void setName(Integer name) {
        this.name = name;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        return name+":"+offset+"\n";
    }
}
