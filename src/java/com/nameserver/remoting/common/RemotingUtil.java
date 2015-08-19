package com.nameserver.remoting.common;


import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class RemotingUtil {

    private static final Logger logger = LoggerFactory.getLogger(RemotingUtil.class);

    public static void closeChannel(Channel channel) {
        final String remoteAddr = RemotingHelper.parseChannelRemoteAddr(channel);
        channel.close().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                logger.info("closeChannel : close the connection to remote address[{}] result : {}", remoteAddr, future.isSuccess());
            }
        });

    }
}
