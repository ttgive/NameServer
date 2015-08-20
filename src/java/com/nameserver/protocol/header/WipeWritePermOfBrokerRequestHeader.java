package com.nameserver.protocol.header;

import com.nameserver.remoting.CommandCustomHeader;
import com.nameserver.remoting.annotaion.CFNotNull;
import com.nameserver.remoting.excpetion.RemotingCommandException;

public class WipeWritePermOfBrokerRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String brokerName;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public String getBrokerName() {
        return brokerName;
    }


    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }
}
