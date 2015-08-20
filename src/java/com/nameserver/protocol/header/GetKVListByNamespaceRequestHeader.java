package com.nameserver.protocol.header;

import com.nameserver.remoting.CommandCustomHeader;
import com.nameserver.remoting.annotaion.CFNotNull;
import com.nameserver.remoting.excpetion.RemotingCommandException;

/**
 * Created by liubotao on 15/8/20.
 */
public class GetKVListByNamespaceRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private String namespace;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
}
