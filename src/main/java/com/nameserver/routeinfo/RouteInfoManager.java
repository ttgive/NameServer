package com.nameserver.routeinfo;


import com.nameserver.common.DataVersion;
import com.nameserver.protocol.BrokerData;
import com.nameserver.protocol.QueueData;
import com.nameserver.protocol.body.ClusterInfo;
import com.nameserver.protocol.body.TopicList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.Channel;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RouteInfoManager {

    private static final Logger logger = LoggerFactory.getLogger(RouteInfoManager.class);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final HashMap<String, List<QueueData>> topicQueueTable;
    private final HashMap<String, BrokerData> brokerAddrTable;
    private final HashMap<String, Set<String>> clusterAddrTable;
    private final HashMap<String, BrokerLiveInfo> brokerLiveTale;
    private final HashMap<String, List<String>> filterServerTable;

    public RouteInfoManager() {
        this.topicQueueTable = new HashMap<String, List<QueueData>>(1024);
        this.brokerAddrTable = new HashMap<String, BrokerData>(128);
        this.clusterAddrTable = new HashMap<String, Set<String>>(32);
        this.brokerLiveTale = new HashMap<String, BrokerLiveInfo>(256);
        this.filterServerTable = new HashMap<String, List<String>>(256);
    }

    public byte[] getAllClusterInfo() {
        ClusterInfo clusterInfoSerializeWrapper = new ClusterInfo();
        clusterInfoSerializeWrapper.setBrokderAddrTable(this.brokerAddrTable);
        clusterInfoSerializeWrapper.setClusterAddrTable(this.clusterAddrTable);
        return clusterInfoSerializeWrapper.encode();
    }

    public void deleteTopic(final String topic) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                this.topicQueueTable.remove(topic);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            logger.error("deleteTopic exception", e);
        }
    }

    public byte[] getAllTopicList() {
        TopicList topicList = new TopicList();
        try {
            try{
                this.lock.readLock().lockInterruptibly();
                topicList.getTopicList().addAll(this.topicQueueTable.keySet());
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            logger.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    public byte[] getTopicByCluster(String cluster) {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Set<String> brokerNameSet
                        = this.clusterAddrTable.get(cluster);
                for (String brokerName : brokerNameSet) {
                    Iterator<Map.Entry<String, List<QueueData>>> topicTableIt
                            = this.topicQueueTable.entrySet().iterator();

                    while(topicTableIt.hasNext()) {
                        Map.Entry<String, List<QueueData>> topicEntry
                                = topicTableIt.next();

                        String topic = topicEntry.getKey();
                        List<QueueData> queueDatas = topicEntry.getValue();
                        for (QueueData queueData : queueDatas) {
                            if (brokerName.equals(queueData.getBrokerName())) {
                                topicList.getTopicList().add(topic);
                                break;
                            }
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            logger.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    public byte[] getSystemTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                for (String clu)
            }
        }
    }


    class BrokerLiveInfo {
        private long lastUpdateTimestamp;
        private DataVersion dataVersion;
        private Channel channel;
        private String haServerAddr;

        public BrokerLiveInfo(long lastUpdateTimestamp,
                              DataVersion dataVersion,
                              Channel channel,
                              String haServerAddr) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
            this.dataVersion = dataVersion;
            this.channel = channel;
            this.haServerAddr = haServerAddr;
        }

        public long getLastUpdateTimestamp() {
            return lastUpdateTimestamp;
        }

        public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
        }

        public DataVersion getDataVersion() {
            return dataVersion;
        }

        public void setDataVersion(DataVersion dataVersion) {
            this.dataVersion = dataVersion;
        }

        public Channel getChannel() {
            return channel;
        }

        public void setChannel(Channel channel) {
            this.channel = channel;
        }

        public String getHaServerAddr() {
            return haServerAddr;
        }

        public void setHaServerAddr(String haServerAddr) {
            this.haServerAddr = haServerAddr;
        }
    }
}
