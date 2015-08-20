package com.nameserver.kvconfig;


import com.nameserver.NamesrvController;
import com.nameserver.common.NamesrvUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.text.html.HTMLDocument;
import java.beans.IntrospectionException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class KVConfigManager {

    private static final Logger logger
            = LoggerFactory.getLogger(KVConfigManager.class);

    private final NamesrvController namesrvController;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final HashMap<String, HashMap<String, String>> configTable =
            new HashMap<String, HashMap<String, String>>();

    public KVConfigManager(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }

    public void load() {
        String content = NamesrvUtil.file2String(this.namesrvController.getNamesrvConfig().getKvConfigPath());
        if (content != null) {
            KVConfigSerializeWrapper kvConfigSerializeWrapper = KVConfigSerializeWrapper.fromJson(content,
                    KVConfigSerializeWrapper.class);

            if (kvConfigSerializeWrapper != null) {
                this.configTable.putAll(kvConfigSerializeWrapper.getConfigTable());
                logger.info("load KV config table OK");
            }
        }
    }

    public void putKVConfig(final String namespace, final String key, final String value) {
        try {
            this.lock.writeLock().lockInterruptibly();
            try {
                HashMap<String, String> kvTable = this.configTable.get(namespace);
                if (kvTable == null) {
                    kvTable = new HashMap<String, String>();
                    this.configTable.put(namespace, kvTable);
                    logger.info("putKVConfig create new Namespace {}", namespace);
                }

                final String prev = kvTable.put(key, value);
                if (prev != null) {
                    logger.info("putKVConfig update config item , Namespace : {} value : {}",
                            namespace, key, value);
                } else {
                    logger.info("putKVConfig create config item, Namespace : {} key: {} value: {}",
                            namespace, key, value);
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            logger.error("putKVConfig InterruptedException", e);
        }

        this.persist();
    }

    public void deleteKVConfig(final String namespace, final String key) {
        try {
            this.lock.writeLock().lockInterruptibly();
            try {
                HashMap<String, String> kvTable = this.configTable.get(namespace);
                if (kvTable != null) {
                    String value = kvTable.remove(key);
                    logger.info("deleteKVConfig delete a config item , Namespace : {} key:{} value: {}",
                            namespace, key, value);
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            logger.error("deleteKVConfig InterruptedException", e);
        }

        this.persist();
    }

    public byte[] getKVListByNamespace(final String namespace) {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                HashMap<String, String> kvTable = this.configTable.get(namespace);
                if (kvTable != null) {
                    KVTable table = new KVtable();
                    table.setTable(kvTable);
                    return table.encode();
                }
            } finally {
                this.lock.writeLock().unlock();
            }

        } catch (InterruptedException e) {
            logger.error("getKVListByNamespace InterruptedException", e);
        }

        return null;
    }

    public String getKVConfig(final String namespace, final String key) {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                HashMap<String, String> kvTable = this.configTable.get(namespace);
                if (kvTable != null) {
                    return kvTable.get(key);
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            logger.error("getKVConfig InterruptedException", e);
        }

        return null;
    }

    public String getKVConfigByValue(final String namespace, final String value) {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                HashMap<String, String> kvTable = this.configTable.get(namespace);
                if (kvTable != null) {
                    StringBuilder sb = new StringBuilder();
                    String splitor = "";
                    for (Map.Entry<String, String> entry : kvTable.entrySet()) {
                        if (value.equals(entry.getValue())) {
                            sb.append(splitor).append(entry.getKey());
                        }
                    }
                    return sb.toString()
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            logger.error("getIpsByProjectGroup InterruptedException", e);
        }

        return null;
    }


    public void deleteKVConfigByValue(final String namespace, final String value) {
        try {
            this.lock.writeLock().lockInterruptibly();
            try {
                HashMap<String, String> kvTable = this.configTable.get(namespace);
                if (kvTable != null) {
                    HashMap<String, String> cloneKvTable = new HashMap<String, String>(kvTable);
                    for (Map.Entry<String, String> entry : cloneKvTable.entrySet()) {
                        if (value.equals(entry.getValue())) {
                            kvTable.remove(entry.getKey());
                        }
                    }

                    logger.info("deleteIpsByProjectGroup delete a config item, Name: {} value : {}",
                            namespace, value);
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            logger.error("deleteIpsByProjectGroup InterruptedException", e);
        }

        this.persist();
    }

    public void persist() {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                KVConfigSerializeWrapper kvConfigSerializeWrapper = new KVConfigSerializeWrapper();
                kvConfigSerializeWrapper.setCOnfigTable(this.configTable);

                String content = kvConfigSerializeWrapper.toJson();

                if (content != null) {
                    NamesrvUtil.string2File(content, this.namesrvController.getNamesrvConfig().getKvConfigPath());
                }
            } catch (IOException e) {
                logger.error("perisist kvconfig exception" + this.namesrvController.getNamesrvConfig().getKvConfigPath(), e);
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            logger.error("perisit InteruptedException", e);
        }
    }


    public void printAllPeriodically() {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                logger.info("----------------------------------");
                {
                    logger.info("configTable size : {}", this.configTable.size());
                    Iterator<Map.Entry<String, HashMap<String, String>>> it = this.configTable.entrySet().iterator();
                    while (it.hasNext()) {

                        Map.Entry<String, HashMap<String, String>> next = it.next();
                        Iterator<Map.Entry<String, String>> itSub = next.getValue().entrySet().iterator();
                        while (itSub.hasNext()) {
                            Map.Entry<String, String> nextSub = itSub.next();
                            logger.info("configTale NS:{} key: {} value:{}", next.getKey(), nextSub.getKey(),
                                    nextSub.getValue());
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            logger.error("printAllPeriodically InterruptedException", e);
        }
    }


}
