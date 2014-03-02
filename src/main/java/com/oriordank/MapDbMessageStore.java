package com.oriordank;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.AbstractMessageStore;
import org.apache.activemq.store.MessageRecoveryListener;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.IOException;

public class MapDbMessageStore extends AbstractMessageStore {
    protected final BTreeMap<MessageId, Message> store;

    protected MessageId lastBatchId;

    public MapDbMessageStore(ActiveMQDestination destination) {
        super(destination);
        DB db = DBMaker.newDirectMemoryDB().transactionDisable().asyncWriteFlushDelay(100).compressionEnable().make();
        store = db.createTreeMap(destination.getQualifiedName()).nodeSize(120).make();
    }

    @Override
    public void addMessage(ConnectionContext connectionContext, Message message) throws IOException {
        System.out.println("Adding message to persistence store");
        store.put(message.getMessageId(), message);
        message.incrementReferenceCount();
    }

    @Override
    public Message getMessage(MessageId messageId) throws IOException {
        return store.get(messageId);
    }

    @Override
    public void removeMessage(ConnectionContext connectionContext, MessageAck messageAck) throws IOException {
        Message removed = store.remove(messageAck.getLastMessageId());
        if (removed != null) {
            removed.decrementReferenceCount();
        }
        if ((lastBatchId != null && lastBatchId.equals(messageAck.getLastMessageId())) || store.isEmpty()) {
            lastBatchId = null;
        }
    }

    @Override
    public void removeAllMessages(ConnectionContext connectionContext) throws IOException {
        store.clear();
    }

    @Override
    public void recover(MessageRecoveryListener messageRecoveryListener) throws Exception {
        for (Message message: store.values()) {
            messageRecoveryListener.recoverMessage(message);
        }
    }

    @Override
    public int getMessageCount() throws IOException {
        return store.size();
    }

    @Override
    public void resetBatching() {
        lastBatchId = null;
    }

    @Override
    public void setBatch(MessageId messageId) {
        lastBatchId = messageId;
    }

    @Override
    public void recoverNextMessages(int i, MessageRecoveryListener messageRecoveryListener) throws Exception {
        boolean pastLackBatch = lastBatchId == null;
        for (Message message : store.values()) {
            if (pastLackBatch) {
                lastBatchId = message.getMessageId();
                messageRecoveryListener.recoverMessage(message);
            } else {
                pastLackBatch = message.getMessageId().equals(lastBatchId);
            }
        }
    }

    public void delete() {
        store.clear();
    }
}
