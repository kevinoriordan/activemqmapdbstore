package com.oriordank;

import org.apache.activemq.store.PList;
import org.apache.activemq.store.PListEntry;
import org.apache.activemq.util.ByteSequence;
import org.mapdb.*;

import java.io.IOException;
import java.util.Iterator;

public class MapDbPList implements PList {

    protected final BTreeMap<Long, PListEntry> store;
    private DB db;
    private String name;
    private long head = Long.MAX_VALUE / 2;
    private long tail = Long.MAX_VALUE / 2;

    public MapDbPList(String name) {
        this.name = name;
        db = DBMaker.newDirectMemoryDB().transactionDisable().asyncWriteFlushDelay(100).compressionEnable().make();
        store = db.createTreeMap(name).nodeSize(120).keySerializer(BTreeKeySerializer.ZERO_OR_POSITIVE_LONG).make();
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
        PListEntry entry = new PListEntry(id, bs, head);
        store.put(head, entry);
        head--;
        return head+1;
    }

    @Override
    public Object addLast(String id, ByteSequence bs) throws IOException {
        PListEntry entry = new PListEntry(id, bs, tail);
        store.put(tail, entry);
        tail++;
        return tail-1;
    }

    @Override
    public boolean remove(Object position) throws IOException {
        PListEntry data = store.remove(position);
        return data != null;
    }

    @Override
    public boolean isEmpty() {
        return store.isEmpty();
    }

    @Override
    public PListIterator iterator() throws IOException {
        final Iterator<PListEntry> pListIterator = store.values().iterator();
        return new PListIterator() {
            @Override
            public void release() {
            }

            @Override
            public boolean hasNext() {
                return pListIterator.hasNext();
            }

            @Override
            public PListEntry next() {
                return pListIterator.next();
            }

            @Override
            public void remove() {
                pListIterator.remove();
            }
        };
    }

    @Override
    public long size() {
        long size = 0;
        for (PListEntry entry: store.values()) {
            size += entry.getByteSequence().getLength();
        }
        return size;
    }
}
