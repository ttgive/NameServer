package com.nameserver.remoting.common;



import io.netty.channel.Channel;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class RemotingHelper {

    public static String exceptionSimpleDesc(final Throwable e) {
        StringBuilder sb = new StringBuilder();
        if (e != null) {
            sb.append(e.toString());

            StackTraceElement[] stackTraceElements = e.getStackTrace();
            if (stackTraceElements != null && stackTraceElements.length > 0) {
                StackTraceElement element = stackTraceElements[0];
                sb.append(", ");
                sb.append(element.toString());
            }
        }

        return sb.toString();
    }

    public static String parseChannelRemoteAddr(final Channel channel) {
        if (channel == null) {
            return "";
        }

        final SocketAddress remote = channel.remoteAddress();
        final String addr = (remote != null ? remote.toString() : "");

        if (addr.length() > 0) {
            int index = addr.lastIndexOf("/");
            if (index > 0) {
                return addr.substring(index + 1);
            }
            return addr;
        }

        return "";
    }

    public static SocketAddress string2SocketAddress(final String addr) {
        String[] s = addr.split(":");
        InetSocketAddress inetSocketAddress = new InetSocketAddress(s[0], Integer.valueOf(s[1]));
        return inetSocketAddress;
    }
}
