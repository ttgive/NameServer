package com.nameserver.remoting;


import com.nameserver.remoting.excpetion.RemotingCommandException;

public interface CommandCustomHeader {
    void checkFields() throws RemotingCommandException;
}
