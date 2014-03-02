package com.oriordank.mapdbserializers;

import org.apache.activemq.command.Message;
import org.mapdb.Serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class MessageSerializer implements Serializer<Message>, Serializable{
    @Override
    public void serialize(DataOutput out, Message value) throws IOException {

    }

    @Override
    public Message deserialize(DataInput in, int available) throws IOException {
        return null;
    }

    @Override
    public int fixedSize() {
        return -1;
    }
}
