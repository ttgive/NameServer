package com.nameserver.protocol.header;

import com.nameserver.remoting.CommandCustomHeader;
import com.nameserver.remoting.annotaion.CFNotNull;
import com.nameserver.remoting.excpetion.RemotingCommandException;

public class UnRegisterBrokerRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String brokerName;
    @CFNotNull
    private String brokerAddr;
    @CFNotNull
    private String clusterName;
    @CFNotNull
    private Long brokerId;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public String getBrokerName() {
        return brokerName;
    }


    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }


    public String getBrokerAddr() {
        return brokerAddr;
    }


    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }


    public String getClusterName() {
        return clusterName;
    }


    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }


    public Long getBrokerId() {
        return brokerId;
    }


    public void setBrokerId(Long brokerId) {
        this.brokerId = brokerId;
    }
}