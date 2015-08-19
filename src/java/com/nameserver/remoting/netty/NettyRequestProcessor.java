package com.nameserver.remoting.netty;

import com.nameserver.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;

public interface NettyRequestProcessor {

    RemotingCommand processRequest(ChannelHandlerContext ctx,
                                   RemotingCommand request) throws Exception;
}
