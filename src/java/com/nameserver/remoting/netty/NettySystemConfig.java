package com.nameserver.remoting.netty;

public class NettySystemConfig {

    public static final String SystemPropertyNettyPooledByteBufAllocatorEnable =
            "com.nameserver.remoting.nettyPooledByteBufAllocatorEnable";

    public static boolean NettyPooledByteBufAllocatoEnable =
            Boolean.parseBoolean(System.getProperty(SystemPropertyNettyPooledByteBufAllocatorEnable, "false"));

    public static final String SystemPropertySocketSndBufSize =
            "com.nameserver.remoting.socket.sndbuf.size";

    public static int SocketSndBufSize =
            Integer.parseInt(System.getProperty(SystemPropertySocketSndBufSize, "65535"));

    public static final String SystemPropertySocketRcvBufSize =
            "com.nameserver.remoting.socket.rcvbuf.size";

    public static int SocketRcvbufSize =
            Integer.parseInt(System.getProperty(SystemPropertySocketRcvBufSize, "65535"));

    public static final String SystemPropertyClientAsyncSemaphoreValue =
            "com.nameserver.remoting.clientAsyncSemaphoreValue";

    public static int ClientAsyncSemaphoreValue =
            Integer.parseInt(System.getProperty(SystemPropertyClientAsyncSemaphoreValue, "2048"));

    public static final String SystemPropertyClientOnewaySemaphoreValue =
            "com.nameserver.remoting.clientOnewaySemaphoreValue";

    public static int ClientOnewaySemaphoreValue =
            Integer.parseInt(System.getProperty(SystemPropertyClientAsyncSemaphoreValue, "2048"));

}
