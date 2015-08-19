package com.nameserver.protocol.body;

import com.nameserver.protocol.BrokerData;
import com.nameserver.remoting.protocol.RemotingSerializable;
import com.sun.corba.se.pept.broker.Broker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class ClusterInfo extends RemotingSerializable {
    private HashMap<String, BrokerData> brokderAddrTable;
    private HashMap<String, Set<String>> clusterAddrTable;

    public HashMap<String, BrokerData> getBrokderAddrTable() {
        return brokderAddrTable;
    }

    public void setBrokderAddrTable(HashMap<String, BrokerData> brokderAddrTable) {
        this.brokderAddrTable = brokderAddrTable;
    }

    public HashMap<String, Set<String>> getClusterAddrTable() {
        return clusterAddrTable;
    }

    public void setClusterAddrTable(HashMap<String, Set<String>> clusterAddrTable) {
        this.clusterAddrTable = clusterAddrTable;
    }

    public String[] retrievAllAddrByCluster(String cluster) {
        List<String> addrs = new ArrayList<String>();
        if (clusterAddrTable.containsKey(cluster)) {
            Set<String> brokerNames = clusterAddrTable.get(cluster);
            for (String brokerName : brokerNames) {
                BrokerData brokerData = brokderAddrTable.get(brokerName);
                if (brokerData != null) {
                    addrs.addAll(brokerData.getBrokerAddrs().values());
                }
            }
        }

        return addrs.toArray(new String[]{});
    }

    public String[] retrieveAllClusterNames() {
        return clusterAddrTable.keySet().toArray(new String[]{});
    }
}
