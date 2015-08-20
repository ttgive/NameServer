package com.nameserver.protocol.header;

import com.nameserver.remoting.CommandCustomHeader;
import com.nameserver.remoting.annotaion.CFNotNull;
import com.nameserver.remoting.excpetion.RemotingCommandException;

public class WipeWritePermOfBrokerResponseHeader implements CommandCustomHeader {
    @CFNotNull
    private Integer wipeTopicCount;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public Integer getWipeTopicCount() {
        return wipeTopicCount;
    }


    public void setWipeTopicCount(Integer wipeTopicCount) {
        this.wipeTopicCount = wipeTopicCount;
    }
}
