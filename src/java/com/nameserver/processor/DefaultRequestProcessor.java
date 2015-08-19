package com.nameserver.processor;


import com.nameserver.NamesrvController;
import com.nameserver.protocol.RequestCode;
import com.nameserver.protocol.ResponseCode;
import com.nameserver.protocol.header.*;
import com.nameserver.remoting.common.RemotingHelper;
import com.nameserver.remoting.excpetion.RemotingCommandException;
import com.nameserver.remoting.excpetion.RemotingConnectException;
import com.nameserver.remoting.excpetion.RemotingException;
import com.nameserver.remoting.netty.NettyRequestProcessor;
import com.nameserver.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultRequestProcessor implements NettyRequestProcessor {

    private static final Logger logger = LoggerFactory.getLogger(DefaultRequestProcessor.class);

    private final NamesrvController namesrvController;

    public DefaultRequestProcessor(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }


    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("receive request, {} {} {}",
                    request.getCode(),
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                    request);
        }

        switch (request.getCode()) {
            case RequestCode.PUT_KV_CONFIG:
                return this.putKVConfig(ctx, request);
            case RequestCode.GET_KV_CONFIG:
                return this.getKVConfig(ctx, request);
        }
    }

    public RemotingCommand putKVConfig(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException{

        final RemotingCommand response =  RemotingCommand.createResponseCommand(null);
        final PutKVConfigRequestHeader requestHeader =
                (PutKVConfigRequestHeader) request.decodeCommandCustomHeader(PutKVConfigRequestHeader.class);

        this.namesrvController.getKvConfigManager().putKVConfig(
                requestHeader.getNamespace(),
                requestHeader.getKey(),
                requestHeader.getValue()
        );

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }

    public RemotingCommand getKVConfig(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingException {
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(GetKVConfigResponseHeader.class);
        final  GetKVConfigResponseHeader responseHeader =
                (GetKVConfigResponseHeader) response.readCustomHeader();
        final GetKVConfigRequestHeader requestHeader =
                (GetKVConfigRequestHeader) request.decodeCommandCustomHeader(GetKVConfigRequestHeader.class);

        String value = this.namesrvController.getKvConfigManager().getKVConfig(
                requestHeader.getNamespace(),
                requestHeader.getKey()
        );

        if (value != null) {
            responseHeader.setValue(value);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        response.setCode(ResponseCode.QUERY_NOT_FOUND);
        response.setRemark("No config item, Namespace :" + requestHeader.getNamespace() +
        " Key :" + requestHeader.getKey());
        return response;
    }

    public RemotingCommand deleteKVConfig(ChannelHandlerContext ctx,
                                          RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final DeleteKVConfigRequestHeader requestHeader =
                (DeleteKVConfigRequestHeader) request.decodeCommandCustomHeader(DeleteKVConfigRequestHeader.class);


        this.namesrvController.getKvConfigManager().deleteKVConfig(
                requestHeader.getNamespace(),
                requestHeader.getKey()
        );

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    public RemotingCommand getKVConfigByValue(ChannelHandlerContext ctx,RemotingCommand request)
        throws RemotingCommandException {
        final  RemotingCommand response = RemotingCommand.createResponseCommand(GetKVConfigResponseHeader.class);
        final  GetKVConfigResponseHeader responseHeader = (GetKVConfigResponseHeader) response.readCustomHeader();
        final  GetKVConfigRequestHeader requestHeader = (GetKVConfigRequestHeader) request.decodeCommandCustomHeader(GetKVConfigRequestHeader.class);

        String value = this.namesrvController.getKvConfigManager().getKVConfigByValue(
                requestHeader.getNamespace(),
                requestHeader.getKey()
        );

        if (value != null) {
            responseHeader.setValue(value);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        response.setCode(ResponseCode.QUERY_NOT_FOUND);
        response.setRemark("No config item, Namespace : " + requestHeader.getNamespace()
        +  " key :" + requestHeader.getKey());

        return response;
    }

    public RemotingCommand deleteKVConfigByValue(ChannelHandlerContext ctx,
                                                 RemotingCommand request)
        throws RemotingCommandException {

        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final DeleteKVConfigRequestHeader requestHeader =
                (DeleteKVConfigRequestHeader) request.decodeCommandCustomHeader(DeleteKVConfigRequestHeader.class);

        this.namesrvController.getKvConfigManager().deleteKVConfigByValue(
                requestHeader.getNamespace(),
                requestHeader.getKey()
        );

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }

    private RemotingCommand getTopicsByCluster(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {

        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetTopicsByClusterRequestHeader requestHeader =
                (GetTopicsByClusterRequestHeader) request.decodeCommandCustomHeader(GetTopicsByClusterRequestHeader.class);


        byte[] body = this.namesrvController.getRouteInfoManager()
                .getTopicByCluster(requestHeader.getCluster());

        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getSystemTopicListFromNs(ChannelHandlerContext ctx, RemotingCommand request)
    throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        byte[] body = this.namesrvController.getRouteInfoManager().getSystemTopicList();

        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

}
