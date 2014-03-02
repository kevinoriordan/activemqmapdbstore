package com.oriordank.mapdbserializers;

import org.apache.activemq.store.PListEntry;
import org.apache.activemq.util.ByteSequence;
import org.mapdb.Serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class PListEntrySerializer implements Serializer<PListEntry>, Serializable {
    @Override
    public void serialize(DataOutput out, PListEntry value) throws IOException {
        out.writeUTF(value.getId());
        ByteSequence bs = value.getByteSequence();
        out.writeInt(bs.getLength());
        out.write(bs.getData(), bs.getOffset(), bs.getLength());
        out.writeLong((Long)value.getLocator());
    }

    @Override
    public PListEntry deserialize(DataInput in, int available) throws IOException {
        String id = in.readUTF();
        int length = in.readInt();
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        ByteSequence bs = new ByteSequence(bytes);
        long locator = in.readLong();
        PListEntry entry = new PListEntry(id, bs, locator);
        return entry;
    }

    @Override
    public int fixedSize() {
        return -1;
    }
}
