package com.nameserver.remoting.netty;


import com.nameserver.remoting.ChannelEventListener;
import com.nameserver.remoting.InvokeCallback;
import com.nameserver.remoting.RPCHook;
import com.nameserver.remoting.common.Pair;
import com.nameserver.remoting.common.RemotingHelper;
import com.nameserver.remoting.common.SemaphoreReleaseOnlyOnce;
import com.nameserver.remoting.common.ServiceThread;
import com.nameserver.remoting.excpetion.RemotingSendRequestException;
import com.nameserver.remoting.excpetion.RemotingTimeoutException;
import com.nameserver.remoting.excpetion.RemotingTooMuchRequestException;
import com.nameserver.remoting.protocol.RemotingCommand;
import com.nameserver.remoting.protocol.RemotingSysResponseCode;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;

public abstract class NettyRemotingAbstract {

    private static final Logger logger = LoggerFactory.getLogger(NettyRemotingAbstract.class);

    protected final Semaphore semaphoreOneway;

    protected final Semaphore semaphoreAsync;

    protected final ConcurrentHashMap<Integer, ResponseFuture> responseTable =
            new ConcurrentHashMap<Integer, ResponseFuture>(256);

    protected Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcess;

    protected final HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>> processTable =
            new HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>>(64);

    protected final NettyEventExecuter nettyEventExecuter = new NettyEventExecuter();

    public abstract ChannelEventListener getChannelEventListener();

    public abstract RPCHook getRPCHook();

    public void putNettyEvent(final NettyEvent event) {
        this.nettyEventExecuter.putNettyEvent(event);
    }


    class NettyEventExecuter extends ServiceThread {
        private final LinkedBlockingDeque<NettyEvent> eventQueue = new LinkedBlockingDeque<NettyEvent>();
        private final int MaxSize = 10000;

        public void putNettyEvent(final NettyEvent event) {
            if (this.eventQueue.size() <= MaxSize) {
                this.eventQueue.add(event);
            } else {
                logger.warn("event queue size[{}] enough, so drop this event {}", this.eventQueue.size(),
                        event.toString());
            }
        }

        @Override
        public void run() {
            logger.info(this.getServiceName() + " service started");
            final ChannelEventListener listener = NettyRemotingAbstract.this.getChannelEventListener();

            while (!this.isStoped()) {
                try {
                    NettyEvent event = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
                    if (event != null && listener != null) {
                        switch (event.getType()) {
                            case IDLE:
                                listener.onChannelIdle(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CLOSE:
                                listener.onChannelClose(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CONNECT:
                                listener.onChannelConnect(event.getRemoteAddr(), event.getChannel());
                                break;
                            case EXCEPTION:
                                listener.onChannelException(event.getRemoteAddr(), event.getChannel());
                                break;
                            default:
                                break;
                        }
                    }
                } catch (Exception e) {
                    logger.warn(this.getServiceName() + " service has exception . ", e);
                }
            }

            logger.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return NettyEventExecuter.class.getSimpleName();
        }
    }


    public NettyRemotingAbstract(final int permitsOneway, final int permitsAsync) {
        this.semaphoreOneway = new Semaphore(permitsOneway, true);
        this.semaphoreAsync = new Semaphore(permitsAsync, true);
    }

    public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
        final Pair<NettyRequestProcessor, ExecutorService> matched = this.processTable.get(cmd.getCode());
        final Pair<NettyRequestProcessor, ExecutorService> pair =
                (matched == null ? this.defaultRequestProcess : matched);

        if (pair != null) {
            Runnable run = new Runnable() {
                @Override
                public void run() {
                    try {
                        RPCHook rpcHook = NettyRemotingAbstract.this.getRPCHook();
                        if (rpcHook != null) {
                            rpcHook.doBeforeRequest(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd);
                        }

                        final RemotingCommand response = pair.getObject1().processRequest(ctx, cmd);
                        if (rpcHook != null) {
                            rpcHook.doAfterResponse(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd, response);
                        }

                        if (!cmd.isOnewayRPC()) {
                            if (response != null) {
                                response.setOpaque(cmd.getOpaque());
                                response.markResponseType();
                                try {
                                    ctx.writeAndFlush(response);
                                } catch (Throwable e) {
                                    logger.error("process request over, bt response failed", e);
                                    logger.error(cmd.toString());
                                    logger.error(response.toString());
                                }
                            } else {

                            }
                        }
                    } catch (Throwable e) {
                        logger.error("process request exception", e);
                        logger.error(cmd.toString());

                        if (!cmd.isOnewayRPC()) {
                            final RemotingCommand response =
                                    RemotingCommand.createResponseCommand(
                                            RemotingSysResponseCode.SYSTEM_ERROR,
                                            RemotingHelper.exceptionSimpleDesc(e)
                                    );

                            response.setOpaque(cmd.getOpaque());
                            ctx.writeAndFlush(response);
                        }
                    }
                }
            };

            try {
                pair.getObject2().submit(run);
            } catch (RejectedExecutionException e) {
                if ((System.currentTimeMillis() % 10000) == 0) {
                    logger.warn(RemotingHelper.parseChannelRemoteAddr(ctx.channel())
                            + ", too many requests and system thread pool busy, RejectedExecutionException "
                            + pair.getObject2().toString()
                            + "request code :" + cmd.getCode());
                }

                if (!cmd.isOnewayRPC()) {
                    final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                            "too many requests and system thread pool busy, please try another server");
                    response.setOpaque(cmd.getOpaque());
                    ctx.writeAndFlush(response);
                }
            }
        } else {
            String error = "request type " + cmd.getCode() + " not supported";
            final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED,
                    error);

            response.setOpaque(cmd.getOpaque());
            ctx.writeAndFlush(response);
            logger.error(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + error);
        }
    }


    public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        final ResponseFuture responseFuture = responseTable.get(cmd.getOpaque());
        if (responseFuture != null) {
            responseFuture.setResponseCommand(cmd);
            responseFuture.release();
            responseTable.remove(cmd.getOpaque());

            if (responseFuture.getInvokeCallback() != null) {
                boolean runInThisThread = false;
                ExecutorService executor = this.getCallbackExecutor();
                if (executor != null) {
                    try {
                        executor.submit(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    responseFuture.executeInvokeCallback();
                                } catch (Throwable e) {
                                    logger.warn("excute callback in executor exception, and callback throw", e);
                                }
                            }
                        });
                    } catch (Exception e) {
                        runInThisThread = true;
                        logger.warn("excute callback in executor exceptin, maybe executor busy", e);
                    }
                } else {
                    runInThisThread = true;
                }

                if (runInThisThread) {
                    try {
                        responseFuture.executeInvokeCallback();
                    } catch (Throwable e) {
                        logger.warn("executeInvokeCallback Exception", e);
                    }
                }
            } else {
                responseFuture.putResponse(cmd);
            }
        } else {
            logger.warn("receive repose , but not matched any request"
                    + RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            logger.warn(cmd.toString());
        }
    }


    public void processMessageRecived(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
        final RemotingCommand cmd = msg;
        if (cmd != null) {
            switch (cmd.getType()) {
                case REQUEST_COMMAND:
                    processRequestCommand(ctx, cmd);
                    break;
                case RESPONSE_COMMAND:
                    processResponseCommand(ctx, cmd);
                    break;
                default:
                    break;
            }
        }
    }

    abstract public ExecutorService getCallbackExecutor();

    public void scanResponseTable() {
        Iterator<Map.Entry<Integer, ResponseFuture>> it = this.responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, ResponseFuture> next = it.next();
            ResponseFuture response = next.getValue();

            if ((response.getBeginTimestamp() + response.getTimeoutMillis() + 100) <= System.currentTimeMillis()) {
                it.remove();
                try {
                    response.executeInvokeCallback();
                } catch (Throwable e) {
                    logger.warn("scanResponseTable, operationComplete Exception", e);
                } finally {
                    response.release();
                }

                logger.warn("remove timeout request, " + response);
            }
        }
    }

    public RemotingCommand invokeSyncImpl(final Channel channel,
                                          final RemotingCommand request,
                                          final long timeoutMillis)
            throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {

        try {
            final ResponseFuture responseFuture = new ResponseFuture(request.getOpaque(), timeoutMillis, null, null);
            this.responseTable.put(request.getOpaque(), responseFuture);
            channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        responseFuture.setSendRequestOK(true);
                        return;
                    } else {
                        responseFuture.setSendRequestOK(false);
                    }

                    responseTable.remove(request.getOpaque());
                    responseFuture.setCause(future.cause());
                    responseFuture.putResponse(null);

                    logger.warn("send a request command to channel < " + channel.remoteAddress() + "> failed");
                    logger.warn(request.toString());
                }
            });

            RemotingCommand remotingCommand = responseFuture.waitResponse(timeoutMillis);
            if (remotingCommand == null) {
                if (responseFuture.isSendRequestOK()) {
                    throw new RemotingTimeoutException(RemotingHelper.parseChannelRemoteAddr(channel),
                            timeoutMillis, responseFuture.getCause());
                } else {
                    throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel),
                            responseFuture.getCause());
                }
            }

            return remotingCommand;
        } finally {
            this.responseTable.remove(request.getOpaque());
        }
    }


    public void invokeAsyncImpl(final Channel channel, final RemotingCommand request,
                                final long timeoutMillis, final InvokeCallback invokeCallback)
            throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException,
            RemotingSendRequestException {
        boolean acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);

        if (acquired) {
            final SemaphoreReleaseOnlyOnce onlyOnce = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);
            final ResponseFuture responseFuture = new ResponseFuture(request.getOpaque(),
                    timeoutMillis, invokeCallback, onlyOnce);

            this.responseTable.put(request.getOpaque(), responseFuture);

            try {
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (future.isSuccess()) {
                            responseFuture.setSendRequestOK(true);
                            return;
                        } else {
                            responseFuture.setSendRequestOK(false);
                        }

                        responseFuture.putResponse(null);
                        responseTable.remove(request.getOpaque());

                        try {
                            responseFuture.executeInvokeCallback();
                        } catch (Throwable e) {
                            logger.warn("excute callback in writeAndFlush addListener, and callback throw", e);
                        } finally {
                            responseFuture.release();
                        }

                        logger.warn("send a request command to channel <{}> failed",
                                RemotingHelper.parseChannelRemoteAddr(channel));
                        logger.warn(request.toString());
                    }
                });
            } catch (Exception e) {
                responseFuture.release();
                logger.warn("send a request command to channel < " + RemotingHelper.parseChannelRemoteAddr(channel) + "> Exception", e);
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeAsyncImpl invoke too fast");
            } else {
                String info =
                        String
                                .format(
                                        "invokeAsyncImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d", //
                                        timeoutMillis,//
                                        this.semaphoreAsync.getQueueLength(),//
                                        this.semaphoreAsync.availablePermits()//
                                );
                logger.warn(info);
                logger.warn(request.toString());
                throw new RemotingTimeoutException(info);
            }
        }
    }


    public void invokeOnewayImpl(final Channel channel, final RemotingCommand request,
                                 final long timeoutMillis) throws InterruptedException,
            RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {

        request.markOnewayRPC();
        boolean acquired = this.semaphoreOneway.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneway);

            try {
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        once.release();
                        if (!future.isSuccess()) {
                            logger.warn("send a request command to chanel < " + channel.remoteAddress()
                                    + " > failed");
                            logger.warn(request.toString());
                        }
                    }
                });
            } catch (Exception e) {
                once.release();
                logger.warn("write send a request command to channel < " + channel.remoteAddress() + "> failed ");
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeOnewayImpl invoke too fast");
            } else {
                String error =
                        String
                                .format(
                                        "invokeOnewayImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d", //
                                        timeoutMillis,//
                                        this.semaphoreAsync.getQueueLength(),//
                                        this.semaphoreAsync.availablePermits()//
                                );

                logger.warn(error);
                logger.warn(request.toString());
                throw new RemotingTimeoutException(error);
            }
        }

    }

}
