package com.nameserver;


public class NameServerVersion {

    // TODO 每次发布版本都要修改此处版本号
    public static final int CurrentVersion = Version.V0_0_0.ordinal();

    public static String getVersionDesc(int value) {
        try {
            Version v = Version.values()[value];
            return v.name();
        } catch (Exception e) {
        }

        return "HigherVersion";
    }


    public static Version value2Version(int value) {
        return Version.values()[value];
    }

    public static enum Version {
        V0_0_0,
    }
}
