package message;

import java.io.Serializable;

public abstract class AbstractMessage implements Serializable {
    public int code;
    public final static int DOWN = 0;
    public final static int STORAGE = 1;
    public final static int DATA_NODE_STATE = 2;
    public final static int STORAGE_INFO = 3;
    public final static int DATA_NODE_REGISTER = 4;
    public final static int NAME_NODE_REGISTER = 5;
    public final static int OK = 6;
    public final static int ERROR = 7;
    public final static int Break_Storage_Response = 8;
    public final static int Break_Storage = 11;
    public final static int Break_DELETE = 12;
    public final static int Break_QUERY= 9;
    public final static int CONTINUE_STORAGE= 13;
    public final static int QUERY_META_ADDRESS= 14;
    public final static int RESPONSE_META_ADDRESS= 15;
    public final static int ONLINE_DECTECT= 16;
    public final static int CONTINUE_DOWNLOAD= 17;
    public final static int CONTINUE_DOWNLOAD_OFFSET= 18;
    public final static int Break_QUERY_Response = 10;
}
