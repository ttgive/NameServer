package com.nameserver.protocol.header;


import com.nameserver.remoting.CommandCustomHeader;
import com.nameserver.remoting.annotaion.CFNullable;
import com.nameserver.remoting.excpetion.RemotingCommandException;

public class GetKVConfigResponseHeader implements CommandCustomHeader {
    @CFNullable
    private String value;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public String getValue() {
        return value;
    }


    public void setValue(String value) {
        this.value = value;
    }
}
