package com.oriordank.plist;

import org.apache.activemq.store.PList;
import org.apache.activemq.store.PListStore;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @org.apache.xbean.XBean element="mapDbTempStore"
 */
public class MapDbPListStore implements PListStore {

    private ConcurrentMap<String, PList> plists = new ConcurrentHashMap<String, PList>();

    @Override
    public File getDirectory() {
        try {
            return File.createTempFile("temp",".tmp").getParentFile();
        } catch (IOException e) {
            return null;
        }
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
