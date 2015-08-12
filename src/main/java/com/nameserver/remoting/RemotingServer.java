package com.nameserver.remoting;

import com.nameserver.remoting.common.Pair;
import com.nameserver.remoting.excpetion.RemotingSendRequestException;
import com.nameserver.remoting.excpetion.RemotingTimeoutException;
import com.nameserver.remoting.excpetion.RemotingTooMuchRequestException;
import com.nameserver.remoting.netty.NettyRequestProcessor;
import com.nameserver.remoting.protocol.RemotingCommand;
import io.netty.channel.Channel;

import java.util.concurrent.ExecutorService;

public interface RemotingServer extends RemotingService {

    public void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
                                  final ExecutorService executor);

    public void registerDefaultProcessor(final NettyRequestProcessor processor, final ExecutorService executor);

    public int localListenPort();

    public Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(final int requestCode);

    public RemotingCommand invokeSync(final Channel channel,
                                      final RemotingCommand request,
                                      final long timeoutMillis)
            throws InterruptedException, RemotingSendRequestException,
            RemotingTimeoutException;


    public void invokeAsync(final Channel channel, final RemotingCommand request,
                            final long timeoutMillis,
                            final InvokeCallback invokeCallback)
            throws InterruptedException, RemotingTooMuchRequestException,
            RemotingTimeoutException, RemotingSendRequestException;

    public void invokeOneway(final Channel channel, final RemotingCommand request,
                             final long timeoutMillis)
            throws InterruptedException, RemotingTooMuchRequestException,
            RemotingTimeoutException, RemotingSendRequestException;
}
