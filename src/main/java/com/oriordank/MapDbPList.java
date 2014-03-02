package com.oriordank;

import org.apache.activemq.store.PList;
import org.apache.activemq.store.PListEntry;
import org.apache.activemq.util.ByteSequence;
import org.mapdb.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class MapDbPList implements PList {

    protected final BTreeMap<String, byte[]> store;
    private DB db;
    private String name;

    public MapDbPList(String name) {
        this.name = name;
        db = DBMaker.newDirectMemoryDB().transactionDisable().asyncWriteFlushDelay(100).compressionEnable().make();
        store = db.createTreeMap(name).nodeSize(120).keySerializer(BTreeKeySerializer.STRING).valueSerializer(Serializer.BYTE_ARRAY).make();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void destroy() throws IOException {
        store.clear();
        db.close();
    }

    @Override
    public Object addFirst(String id, ByteSequence bs) throws IOException {
        store.put(id, bs.getData());
        return id;
    }

    @Override
    public Object addLast(String id, ByteSequence bs) throws IOException {
        store.put(id, bs.getData());
        return id;
    }

    @Override
    public boolean remove(Object position) throws IOException {
        byte[] data = store.remove(position);
        return data != null;
    }

    @Override
    public boolean isEmpty() {
        return store.isEmpty();
    }

    @Override
    public PListIterator iterator() throws IOException {
        final Iterator<Map.Entry<String, byte[]>> plistIterator = store.entrySet().iterator();
        return new PListIterator() {
            @Override
            public void release() {
            }

            @Override
            public boolean hasNext() {
                return plistIterator.hasNext();
            }

            @Override
            public PListEntry next() {
                Map.Entry<String, byte[]> next = plistIterator.next();
                return new PListEntry(next.getKey(), new ByteSequence(next.getValue()), next.getKey());
            }

            @Override
            public void remove() {
                plistIterator.remove();
            }
        };
    }

    @Override
    public long size() {
        long size = 0;
        for (byte[] bytes : store.values()) {
            size += bytes.length;
        }
        return size;
    }
}
