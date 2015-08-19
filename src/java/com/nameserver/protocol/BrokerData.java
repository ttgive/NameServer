package com.nameserver.protocol;

import java.util.HashMap;

public class BrokerData implements Comparable<BrokerData> {
    private String brokerName;
    private HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs;


    /**
     * 优先获取Master，如果没有Master尝试找Slave
     */
    public String selectBrokerAddr() {
        String value = this.brokerAddrs.get(MixAll.MASTER_ID);
        if (null == value) {
            for (Long key : this.brokerAddrs.keySet()) {
                return this.brokerAddrs.get(key);
            }
        }

        return value;
    }


    public String getBrokerName() {
        return brokerName;
    }


    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }


    public HashMap<Long, String> getBrokerAddrs() {
        return brokerAddrs;
    }


    public void setBrokerAddrs(HashMap<Long, String> brokerAddrs) {
        this.brokerAddrs = brokerAddrs;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerAddrs == null) ? 0 : brokerAddrs.hashCode());
        result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        BrokerData other = (BrokerData) obj;
        if (brokerAddrs == null) {
            if (other.brokerAddrs != null)
                return false;
        }
        else if (!brokerAddrs.equals(other.brokerAddrs))
            return false;
        if (brokerName == null) {
            if (other.brokerName != null)
                return false;
        }
        else if (!brokerName.equals(other.brokerName))
            return false;
        return true;
    }


    @Override
    public String toString() {
        return "BrokerData [brokerName=" + brokerName + ", brokerAddrs=" + brokerAddrs + "]";
    }


    @Override
    public int compareTo(BrokerData o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }
}