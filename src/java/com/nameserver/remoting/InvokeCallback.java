package com.nameserver.remoting;


import com.nameserver.remoting.netty.ResponseFuture;

public interface InvokeCallback {
    public void operationComplete(final ResponseFuture responseFuture);
}
