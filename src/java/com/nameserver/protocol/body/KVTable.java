package com.nameserver.protocol.body;

import com.nameserver.remoting.protocol.RemotingSerializable;

import java.util.HashMap;

public class KVTable extends RemotingSerializable {
    private HashMap<String, String> table = new HashMap<String, String>();


    public HashMap<String, String> getTable() {
        return table;
    }


    public void setTable(HashMap<String, String> table) {
        this.table = table;
    }
}
