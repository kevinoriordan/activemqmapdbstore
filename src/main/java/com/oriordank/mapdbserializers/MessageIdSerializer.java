package com.oriordank.mapdbserializers;

import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.SessionId;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DataOutput2;

import javax.jms.Session;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;

public class MessageIdSerializer extends BTreeKeySerializer<MessageId> implements Serializable {
    @Override
    public void serialize(DataOutput out, int start, int end, Object[] keys) throws IOException {
        for (int i=start; i<end; i++) {
            MessageId message = (MessageId)keys[i];
            DataOutput2.packLong(out, message.getProducerSequenceId());
            DataOutput2.packLong(out, message.getProducerId().getSessionId());
            DataOutput2.packLong(out, message.getProducerId().getValue());
            out.writeUTF(message.getProducerId().getConnectionId());
        }
    }

    @Override
    public Object[] deserialize(DataInput in, int start, int end, int size) throws IOException {
        long producerSequenceId = in.readLong();
        long sessionIdValue = in.readLong();
        long producerIdValue = in.readLong();
        SessionId sessionId = new SessionId();
        ProducerId producerId = new ProducerId();
        MessageId messageId = new MessageId();
        return new Object[] {messageId};
    }

    @Override
    public Comparator<MessageId> getComparator() {
        return null;
    }
}
