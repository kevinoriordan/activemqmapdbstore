package com.oriordank;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.store.*;
import org.apache.activemq.store.memory.MemoryTopicMessageStore;
import org.apache.activemq.store.memory.MemoryTransactionStore;
import org.apache.activemq.usage.SystemUsage;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MapDbPersistenceAdapter implements PersistenceAdapter, BrokerServiceAware {

    private ConcurrentMap<ActiveMQDestination, MessageStore> mapDbStores = new ConcurrentHashMap<ActiveMQDestination, MessageStore>();

    MemoryTransactionStore transactionStore;

    @Override
    public void setBrokerService(BrokerService brokerService) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Set<ActiveMQDestination> getDestinations() {
        return new HashSet<ActiveMQDestination>(mapDbStores.keySet());
    }

    @Override
    public MessageStore createQueueMessageStore(ActiveMQQueue activeMQQueue) throws IOException {
        MessageStore store = mapDbStores.get(activeMQQueue);
        if (store == null) {
            store = new MapDbMessageStore(activeMQQueue);
            if (transactionStore != null) {
                store = transactionStore.proxy(store);
            }
        }
        mapDbStores.put(activeMQQueue, store);
        return store;
    }

    @Override
    public TopicMessageStore createTopicMessageStore(ActiveMQTopic activeMQTopic) throws IOException {
        TopicMessageStore rc = (TopicMessageStore) mapDbStores.get(activeMQTopic);
        if (rc == null) {
            rc = new MemoryTopicMessageStore(activeMQTopic);
            if (transactionStore != null) {
                rc = transactionStore.proxy(rc);
            }
            mapDbStores.put(activeMQTopic, rc);
        }
        return rc;
    }

    @Override
    public void removeQueueMessageStore(ActiveMQQueue activeMQQueue) {
        mapDbStores.remove(activeMQQueue);
    }

    @Override
    public void removeTopicMessageStore(ActiveMQTopic activeMQTopic) {
        mapDbStores.remove(activeMQTopic);
    }

    @Override
    public TransactionStore createTransactionStore() throws IOException {
        if (transactionStore == null) {
            transactionStore = new MemoryTransactionStore(this);
        }
        return transactionStore;
    }

    @Override
    public void beginTransaction(ConnectionContext connectionContext) throws IOException {
    }

    @Override
    public void commitTransaction(ConnectionContext connectionContext) throws IOException {
    }

    @Override
    public void rollbackTransaction(ConnectionContext connectionContext) throws IOException {
    }

    @Override
    public long getLastMessageBrokerSequenceId() throws IOException {
        return 0;
    }

    @Override
    public void deleteAllMessages() throws IOException {
        for (MessageStore messageStore : mapDbStores.values()) {
            if (messageStore != null) {
                MapDbMessageStore mapDbMessageStore = extractMapDbMessageStore(messageStore);
                mapDbMessageStore.delete();
            }
        }
        if (transactionStore != null) {
            transactionStore.delete();;
        }
    }

    private MapDbMessageStore extractMapDbMessageStore(MessageStore messageStore) {
        if (messageStore instanceof MapDbMessageStore) {
            return (MapDbMessageStore)messageStore;
        }
        if (messageStore instanceof ProxyMessageStore) {
            MessageStore delegate = ((ProxyMessageStore)messageStore).getDelegate();
            if (delegate instanceof MapDbMessageStore) {
                return (MapDbMessageStore) delegate;
            }
        }
        return null;
    }

    @Override
    public void setUsageManager(SystemUsage systemUsage) {
    }

    @Override
    public void setBrokerName(String s) {
    }

    @Override
    public void setDirectory(File file) {
    }

    @Override
    public File getDirectory() {
        return null;
    }

    @Override
    public void checkpoint(boolean b) throws IOException {
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public long getLastProducerSequenceId(ProducerId producerId) throws IOException {
        return -1;
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void stop() throws Exception {
    }
}
