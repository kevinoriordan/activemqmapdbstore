package com.oriordank;

import com.oriordank.plist.MapDbPListStore;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;

public class MapDbBrokerPlugin implements BrokerPlugin {
    @Override
    public Broker installPlugin(Broker broker) throws Exception {
        broker.getBrokerService().setTempDataStore(new MapDbPListStore());
        return broker;
    }
}
