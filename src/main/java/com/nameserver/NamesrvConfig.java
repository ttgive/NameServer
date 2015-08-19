package com.nameserver;

import java.io.File;

public class NamesrvConfig {

    private String rocketmqHome = "";

    private String kvConfigPath = System.getProperty("user.home")
            + File.separator + "namesrv" + File.separator + "kvConfig.json";

    public String getRocketmqHome() {
        return rocketmqHome;
    }

    public void setRocketmqHome(String rocketmqHome) {
        this.rocketmqHome = rocketmqHome;
    }

    public String getKvConfigPath() {
        return kvConfigPath;
    }

    public void setKvConfigPath(String kvConfigPath) {
        this.kvConfigPath = kvConfigPath;
    }
}
