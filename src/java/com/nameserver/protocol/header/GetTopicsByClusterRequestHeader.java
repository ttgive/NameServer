package com.nameserver.protocol.header;


import com.nameserver.remoting.CommandCustomHeader;
import com.nameserver.remoting.annotaion.CFNotNull;
import com.nameserver.remoting.excpetion.RemotingCommandException;

public class GetTopicsByClusterRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String cluster;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public String getCluster() {
        return cluster;
    }


    public void setCluster(String cluster) {
        this.cluster = cluster;
    }
}
