package com.oriordank.unit;

import com.oriordank.plist.MapDbPListStore;
import com.oriordank.MapDbPersistenceAdapter;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.PersistenceAdapterTestSupport;

public class MapDbPersistenceAdapterTest extends PersistenceAdapterTestSupport {
    @Override
    protected PersistenceAdapter createPersistenceAdapter(boolean b) throws Exception {
        return new MapDbPersistenceAdapter();
    }

    @Override
    public void setUp() throws Exception {
        brokerService = new BrokerService();
        pa = createPersistenceAdapter(true);
        brokerService.setPersistenceAdapter(pa);
        brokerService.setTempDataStore(new MapDbPListStore());
        brokerService.start();
    }
}
