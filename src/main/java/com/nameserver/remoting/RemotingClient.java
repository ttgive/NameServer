package com.nameserver.remoting;


import com.nameserver.remoting.excpetion.RemotingConnectException;
import com.nameserver.remoting.excpetion.RemotingSendRequestException;
import com.nameserver.remoting.excpetion.RemotingTimeoutException;
import com.nameserver.remoting.excpetion.RemotingTooMuchRequestException;
import com.nameserver.remoting.netty.NettyRequestProcessor;
import com.nameserver.remoting.protocol.RemotingCommand;

import java.util.List;
import java.util.concurrent.ExecutorService;

public interface RemotingClient extends RemotingService {

    public void updateNameServerddressList(final List<String> addrs);

    public List<String> getNameServerAddressList();

    public RemotingCommand invokeSync(final String addr, final RemotingCommand request,
                                      final long timeoutMillis) throws InterruptedException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException;


    public void invokeAsync(final String addr, final RemotingCommand request, final long timeoutMillis,
                            final InvokeCallback invokeCallback) throws InterruptedException, RemotingConnectException,
            RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;


    public void invokeOneway(final String addr, final RemotingCommand request, final long timeoutMillis)
            throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException,
            RemotingTimeoutException, RemotingSendRequestException;


    public void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
                                  final ExecutorService executor);

    public boolean isChannelWriteable(final String addr);

}
