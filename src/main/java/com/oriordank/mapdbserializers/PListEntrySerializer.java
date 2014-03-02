package com.oriordank.mapdbserializers;

import org.apache.activemq.store.PListEntry;
import org.apache.activemq.util.ByteSequence;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
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
        DataOutput2.packInt(out, bs.getLength());
        out.write(bs.getData(), bs.getOffset(), bs.getLength());
        DataOutput2.packLong(out, (Long)value.getLocator());
    }

    @Override
    public PListEntry deserialize(DataInput in, int available) throws IOException {
        String id = in.readUTF();
        int length = DataInput2.unpackInt(in);
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        ByteSequence bs = new ByteSequence(bytes);
        long locator = DataInput2.unpackLong(in);
        PListEntry entry = new PListEntry(id, bs, locator);
        return entry;
    }

    @Override
    public int fixedSize() {
        return -1;
    }
}
