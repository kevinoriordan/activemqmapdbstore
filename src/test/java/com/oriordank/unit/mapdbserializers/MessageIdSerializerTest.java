package com.oriordank.unit.mapdbserializers;

import com.oriordank.mapdbserializers.MessageIdSerializer;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.SessionId;
import org.junit.Ignore;
import org.junit.Test;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;

import java.io.IOException;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class MessageIdSerializerTest extends SerializerTests {

    @Test
    @Ignore
    public void testSerializeDeserializeMessageId() throws IOException {
        MessageId messageId = new MessageId();
        messageId.setBrokerSequenceId(1l);
        messageId.setProducerSequenceId(2l);
        SessionId sessionId = new SessionId();
        sessionId.setConnectionId("Test");
        sessionId.setValue(3l);
        ProducerId producerId = new ProducerId(sessionId, 4l);
        messageId.setProducerId(producerId);

        MessageIdSerializer messageIdSerializer = new MessageIdSerializer();
        DataOutput2 dataOutput = new DataOutput2();
        messageIdSerializer.serialize(dataOutput, 0, 1, new Object[]{messageId});
        DataInput2 dataInput = new DataInput2(dataOutput.copyBytes());
        Object[] deserializedMessages = messageIdSerializer.deserialize(dataInput, 0, 1, dataInput.available());
        assertThat(deserializedMessages, arrayWithSize(1));

        MessageId deserializedMessage = (MessageId)deserializedMessages[0];
        assertThat(deserializedMessage.getBrokerSequenceId(), is(1l));
        assertThat(deserializedMessage.getProducerSequenceId(), is(2l));
        assertThat(deserializedMessage.getProducerId().getConnectionId(), is("Test"));
        assertThat(deserializedMessage.getProducerId().getValue(), is(4l));
        assertThat(deserializedMessage.getProducerId().getSessionId(), is(3l));
    }
}
