package com.oriordank;

import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.PersistenceAdapterFactory;

import java.io.IOException;

public class MapDbPersistenceAdapterFactory implements PersistenceAdapterFactory {
    @Override
    public PersistenceAdapter createPersistenceAdapter() throws IOException {
        return new MapDbPersistenceAdapter();
    }
}
