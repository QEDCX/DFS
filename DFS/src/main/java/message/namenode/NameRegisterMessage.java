package message.namenode;

import message.AbstractMessage;

public class NameRegisterMessage extends AbstractMessage {
    public Integer listenPort;
    public NameRegisterMessage(Integer listenPort)
    {
        code = NAME_NODE_REGISTER;
        this.listenPort = listenPort;
    }
}
