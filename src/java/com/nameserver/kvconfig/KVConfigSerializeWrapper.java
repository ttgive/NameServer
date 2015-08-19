package com.nameserver.kvconfig;


import com.nameserver.remoting.protocol.RemotingSerializable;

import java.util.HashMap;

public class KVConfigSerializeWrapper extends RemotingSerializable {

    private HashMap<String, HashMap<String, String>> configTable;

    public HashMap<String, HashMap<String, String>> getConfigTable() {
        return configTable;
    }

    public void setConfigTable(HashMap<String, HashMap<String, String>> configTable) {
        this.configTable = configTable;
    }
}
