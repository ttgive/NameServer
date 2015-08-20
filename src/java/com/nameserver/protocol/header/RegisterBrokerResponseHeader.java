package com.nameserver.protocol.header;

import com.nameserver.remoting.CommandCustomHeader;
import com.nameserver.remoting.annotaion.CFNotNull;
import com.nameserver.remoting.excpetion.RemotingCommandException;

/**
 * Created by liubotao on 15/8/20.
 */
public class RegisterBrokerResponseHeader implements CommandCustomHeader {

    @CFNotNull
    private String haServerAddr;

    @CFNotNull
    private String masterAddr;


    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getHaServerAddr() {
        return haServerAddr;
    }

    public void setHaServerAddr(String haServerAddr) {
        this.haServerAddr = haServerAddr;
    }

    public String getMasterAddr() {
        return masterAddr;
    }

    public void setMasterAddr(String masterAddr) {
        this.masterAddr = masterAddr;
    }
}
