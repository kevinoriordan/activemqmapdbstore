package com.oriordank;

import org.apache.activemq.store.PList;
import org.apache.activemq.store.PListStore;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MapDbPListStore implements PListStore {

    private ConcurrentMap<String, PList> plists = new ConcurrentHashMap<String, PList>();

    @Override
    public File getDirectory() {
        return null;
    }

    @Override
    public void setDirectory(File directory) {
    }

    @Override
    public PList getPList(String name) throws Exception {
        PList plist = plists.get(name);
        if (plist == null) {
            plist = new MapDbPList(name);
        }
        return plist;
    }

    @Override
    public boolean removePList(String name) throws Exception {
        PList plist = plists.remove(name);
        return plist != null;
    }

    @Override
    public long size() {
        long size = 0;
        for (PList plist: plists.values()) {
            size += plist.size();
        }
        return size;
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }
}
