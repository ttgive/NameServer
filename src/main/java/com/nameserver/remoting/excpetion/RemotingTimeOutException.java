package com.nameserver.remoting.excpetion;

public class RemotingTimeOutException extends RemotingException {

    private static final long serialVersionUID = 4106899185095245979L;

    public RemotingTimeOutException(String message) {
        super(message);
    }

    public RemotingTimeOutException(String addr, long timeoutMillis) {
        this(addr, timeoutMillis, null);
    }

    public RemotingTimeOutException(String addr, long timeoutMillis, Throwable cause) {
        super("wait response on the channel <" + addr + "> timeout, " + timeoutMillis + "(ms)", cause);
    }
}
