package com.oriordank;

import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageRecoveryListener;
import org.mapdb.BTreeMap;

import java.util.Iterator;
import java.util.Map;

public class MapDbTopicSub {

    private BTreeMap<MessageId, Message> map;
    private MessageId lastBatch;

    void addMessage(MessageId id, Message message) {
        synchronized(this) {
            map.put(id, message);
        }
        message.incrementReferenceCount();
    }

    void removeMessage(MessageId id) {
        Message removed;
        synchronized(this) {
            removed = map.remove(id);
            if ((lastBatch != null && lastBatch.equals(id)) || map.isEmpty()) {
                resetBatching();
            }
        }
        if( removed!=null ) {
            removed.decrementReferenceCount();
        }
    }

    synchronized int size() {
        return map.size();
    }

    synchronized void recoverSubscription(MessageRecoveryListener listener) throws Exception {
        for (Iterator iter = map.entrySet().iterator(); iter.hasNext();) {
            Map.Entry entry = (Map.Entry)iter.next();
            Object msg = entry.getValue();
            if (msg.getClass() == MessageId.class) {
                listener.recoverMessageReference((MessageId)msg);
            } else {
                listener.recoverMessage((Message)msg);
            }
        }
    }

    synchronized void recoverNextMessages(int maxReturned, MessageRecoveryListener listener) throws Exception {
        boolean pastLackBatch = lastBatch == null;
        MessageId lastId = null;
        // the message table is a synchronizedMap - so just have to synchronize
        // here
        int count = 0;
        for (Iterator iter = map.entrySet().iterator(); iter.hasNext() && count < maxReturned;) {
            Map.Entry entry = (Map.Entry)iter.next();
            if (pastLackBatch) {
                count++;
                Object msg = entry.getValue();
                lastId = (MessageId)entry.getKey();
                if (msg.getClass() == MessageId.class) {
                    listener.recoverMessageReference((MessageId)msg);
                } else {
                    listener.recoverMessage((Message)msg);
                }
            } else {
                pastLackBatch = entry.getKey().equals(lastBatch);
            }
        }
        if (lastId != null) {
            lastBatch = lastId;
        }

    }

    synchronized void resetBatching() {
        lastBatch = null;
    }

}
