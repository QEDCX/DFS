package message;

import message.AbstractMessage;

import java.net.InetSocketAddress;

public class MetaAddressResponse extends AbstractMessage {
    public InetSocketAddress metaAddress;

    public MetaAddressResponse(InetSocketAddress metaAddress) {
        this.code = AbstractMessage.RESPONSE_META_ADDRESS;
        this.metaAddress = metaAddress;
    }
}
