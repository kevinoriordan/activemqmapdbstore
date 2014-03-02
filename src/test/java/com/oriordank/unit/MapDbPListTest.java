package com.oriordank.unit;

import com.oriordank.plist.MapDbPListStore;
import org.apache.activemq.store.PListTestSupport;

public class MapDbPListTest extends PListTestSupport {

    @Override
    protected MapDbPListStore createPListStore() {
        return new MapDbPListStore();
    }

    protected MapDbPListStore createConcurrentAddIteratePListStore() {
        return new MapDbPListStore();
    }

    @Override
    protected MapDbPListStore createConcurrentAddRemovePListStore() {
        return new MapDbPListStore();
    }

    @Override
    protected MapDbPListStore createConcurrentAddRemoveWithPreloadPListStore() {
        return new MapDbPListStore();
    }

    @Override
    protected MapDbPListStore createConcurrentAddIterateRemovePListStore(boolean enablePageCache) {
        return new MapDbPListStore();
    }

}
