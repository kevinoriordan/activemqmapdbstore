package com.oriordank;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.*;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.util.SubscriptionKey;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MapDbTopicMessageStore extends MapDbMessageStore implements TopicMessageStore {
    private Map<SubscriptionKey, SubscriptionInfo> subscriberDatabase;
    private Map<SubscriptionKey, MapDbTopicSub> topicSubMap;

    public MapDbTopicMessageStore(ActiveMQDestination destination) {
        super(destination);
        this.subscriberDatabase = subscriberDatabase;
        this.topicSubMap = Collections.synchronizedMap(new HashMap<SubscriptionKey, MapDbTopicSub>());
    }

    @Override
    public void acknowledge(ConnectionContext connectionContext, String clientId, String subscriptionName, MessageId messageId, MessageAck messageAck) throws IOException {
        SubscriptionKey key = new SubscriptionKey(clientId, subscriptionName);
        MapDbTopicSub sub = topicSubMap.get(key);
        if (sub != null) {
            sub.removeMessage(messageId);
        }
    }

    @Override
    public void deleteSubscription(String clientId, String subscriptionName) throws IOException {
        SubscriptionKey key = new SubscriptionKey(clientId, subscriptionName);
        subscriberDatabase.remove(key);
        topicSubMap.remove(key);
    }

    @Override
    public void recoverSubscription(String clientId, String subscriptionName, MessageRecoveryListener messageRecoveryListener) throws Exception {
        MapDbTopicSub sub = topicSubMap.get(new SubscriptionKey(clientId, subscriptionName));
        if (sub != null) {
            sub.recoverSubscription(messageRecoveryListener);
        }
    }

    @Override
    public void recoverNextMessages(String clientId, String subscriptionName, int maxReturned, MessageRecoveryListener messageRecoveryListener) throws Exception {
        MapDbTopicSub sub = this.topicSubMap.get(new SubscriptionKey(clientId, subscriptionName));
        if (sub != null) {
            sub.recoverNextMessages(maxReturned, messageRecoveryListener);
        }
    }

    @Override
    public void resetBatching(String clientId, String subscriptionName) {
        MapDbTopicSub sub = topicSubMap.get(new SubscriptionKey(clientId, subscriptionName));
        if (sub != null) {
            sub.resetBatching();
        }
    }

    @Override
    public int getMessageCount(String clientId, String subscriberName) throws IOException {
        int result = 0;
        MapDbTopicSub sub = topicSubMap.get(new SubscriptionKey(clientId, subscriberName));
        if (sub != null) {
            result = sub.size();
        }
        return result;
    }

    @Override
    public SubscriptionInfo lookupSubscription(String clientId, String subscriptionName) throws IOException {
        return subscriberDatabase.get(new SubscriptionKey(clientId, subscriptionName));
    }

    @Override
    public SubscriptionInfo[] getAllSubscriptions() throws IOException {
        return subscriberDatabase.values().toArray(new SubscriptionInfo[subscriberDatabase.size()]);
    }

    @Override
    public void addSubsciption(SubscriptionInfo subscriptionInfo, boolean retroactive) throws IOException {
        SubscriptionKey key = new SubscriptionKey(subscriptionInfo);
        MapDbTopicSub sub = new MapDbTopicSub();
        topicSubMap.put(key, sub);
        if (retroactive) {
            for (Iterator i = store.entrySet().iterator(); i.hasNext();) {
                Map.Entry entry = (Map.Entry)i.next();
                sub.addMessage((MessageId)entry.getKey(), (Message)entry.getValue());
            }
        }
        subscriberDatabase.put(key, subscriptionInfo);
    }
}
