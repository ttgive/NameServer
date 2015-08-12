package com.nameserver.remoting.netty;


import com.nameserver.remoting.ChannelEventListener;
import com.nameserver.remoting.InvokeCallback;
import com.nameserver.remoting.RPCHook;
import com.nameserver.remoting.RemotingClient;
import com.nameserver.remoting.common.Pair;
import com.nameserver.remoting.common.RemotingHelper;
import com.nameserver.remoting.common.RemotingUtil;
import com.nameserver.remoting.excpetion.RemotingConnectException;
import com.nameserver.remoting.excpetion.RemotingSendRequestException;
import com.nameserver.remoting.excpetion.RemotingTimeoutException;
import com.nameserver.remoting.excpetion.RemotingTooMuchRequestException;
import com.nameserver.remoting.protocol.RemotingCommand;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;


import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class NettyRemotingClient extends NettyRemotingAbstract implements RemotingClient {

    private static final Logger logger = LoggerFactory.getLogger(NettyRemotingClient.class);

    private static final long LockTimeoutMillis = 3000;

    private final NettyClientConfig nettyClientConfig;
    private final Bootstrap bootstrap = new Bootstrap();
    private final EventLoopGroup eventLoopGroupWorker;
    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    private final Lock lockChannelTables = new ReentrantLock();
    private final ConcurrentHashMap<String, ChannelWrapper> channelTables =
            new ConcurrentHashMap<String, ChannelWrapper>();

    private final Timer timer = new Timer("ClientHouseKeepingService", true);

    private final AtomicReference<List<String>> namesrvAddrList = new AtomicReference<List<String>>();
    private final AtomicReference<String> namesrvAddrChoosed = new AtomicReference<String>();
    private final AtomicInteger namesrvIndex = new AtomicInteger(initValueIndex());
    private final Lock lockNamesrvChannel = new ReentrantLock();

    private final ExecutorService publicExecutor;

    private final ChannelEventListener channelEventListener;

    private RPCHook rpcHook;

    class ChannelWrapper {
        private final ChannelFuture channelFuture;

        public ChannelWrapper(ChannelFuture channelFuture) {
            this.channelFuture = channelFuture;
        }

        public boolean isOK() {
            return (this.channelFuture.channel() != null && this.channelFuture.channel().isActive());
        }

        public boolean isWritable() {
            return this.channelFuture.channel().isWritable();
        }

        private Channel getChannel() {
            return this.channelFuture.channel();
        }

        public ChannelFuture getChannelFuture() {
            return channelFuture;
        }
    }


    class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageRecived(ctx, msg);
        }
    }

    class NettyConnectManageHandler extends ChannelDuplexHandler {


        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise future) throws Exception {
            final String local = (localAddress == null ? "UNKNOW" : localAddress.toString());
            final String remote = (remoteAddress == null ? "UNKNOW" : remoteAddress.toString());
            logger.info("Netty client pipeline : connect {} => {}", local, remote);
            super.connect(ctx, remoteAddress, localAddress, future);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remoteAddress.toString(), ctx.channel()));
            }
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise future) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            logger.info("netty client pipeline : disconnect {}", remoteAddress);
            closeChannel(ctx.channel());
            super.disconnect(ctx, future);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE,
                        remoteAddress.toString(),
                        ctx.channel()));
            }
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise future) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            logger.info("netty client pipeline : close {}", remoteAddress);
            closeChannel(ctx.channel());
            super.close(ctx, future);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE,
                        remoteAddress.toString(),
                        ctx.channel()));
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            logger.warn("netty client pipeline : exceptionCaught {}", remoteAddress);
            logger.warn("netty client pipeline : exceptionCaught exception", cause);
            closeChannel(ctx.channel());

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(
                        NettyEventType.EXCEPTION,
                        remoteAddress.toString(),
                        ctx.channel()
                ));
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    logger.warn("netty client pipeline : idle exception [{}]", remoteAddress);
                    closeChannel(ctx.channel());
                    if (NettyRemotingClient.this.channelEventListener != null) {
                        NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.IDLE,
                                remoteAddress.toString(), ctx.channel()));
                    }
                }
            }
            ctx.fireUserEventTriggered(evt);
        }
    }

    private static int initValueIndex() {
        Random r = new Random();
        return Math.abs(r.nextInt() % 999) % 999;
    }

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null);
    }

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig,
                               final ChannelEventListener channelEventListener) {

        super(nettyClientConfig.getClientOnewaySemaphoreValue(), nettyClientConfig
                .getClientAsyncSemaphoreValue());
        this.nettyClientConfig = nettyClientConfig;
        this.channelEventListener = channelEventListener;

        int publicThreadNums = nettyClientConfig.getClientCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums,
                new ThreadFactory() {
                    private AtomicInteger threadIndex = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "NettyClientPublicExecutor_" + this.threadIndex.incrementAndGet());
                    }
                });

        this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyClientSelector_%d",
                        this.threadIndex.incrementAndGet()));
            }
        });
    }

    @Override
    public void start() {
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
                nettyClientConfig.getClientWorkerThreads(),

                new ThreadFactory() {

                    private AtomicInteger threadIndex = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "NettyClientWorkerThread_" + this.threadIndex.incrementAndGet());
                    }
                });

        Bootstrap handler = this.bootstrap.group(this.eventLoopGroupWorker)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.SO_SNDBUF, nettyClientConfig.getClientSocketSndBufSize())
                .option(ChannelOption.SO_RCVBUF, nettyClientConfig.getClientSocketRcvBufSize())
                .handler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(
                                defaultEventExecutorGroup,
                                new NettyEncoder(),
                                new NettyDecoder(),
                                new IdleStateHandler(0, 0, nettyClientConfig.getClientChannelMaxIdleTimeSeconds()),
                                new NettyConnectManageHandler(),
                                new NettyClientHandler()
                        );
                    }
                });

        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    NettyRemotingClient.this.scanResponseTable();
                } catch (Exception e) {
                    logger.error("scanResponseTable exception", e);
                }
            }
        }, 1000 * 3, 1000);

        if (this.channelEventListener != null) {
            this.nettyEventExecuter.start();
        }
    }


    @Override
    public void shutdown() {
        try {
            this.timer.cancel();

            for (ChannelWrapper channelWrapper : this.channelTables.values()) {
                this.closeChannel(null, channelWrapper.getChannel());
            }

            this.channelTables.clear();

            if (this.nettyEventExecuter != null) {
                this.nettyEventExecuter.shutdown();
            }

            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            logger.error("NettyRemotingClient shutdown exception", e);
        }

        if (this.publicExecutor != null) {
            try {
                this.publicExecutor.shutdown();
            } catch (Exception e) {
                logger.error("NettyRemotingServer shutdown exception", e);
            }
        }
    }

    private Channel getAndCreateChannel(final String addr) throws InterruptedException {
        if (addr == null) {
            return getAndCreateNameserverChannel();
        }

        ChannelWrapper channelWrapper = this.channelTables.get(addr);
        if (channelWrapper != null && channelWrapper.isOK()) {
            return channelWrapper.getChannel();
        }

        return this.createChannel(addr);
    }
    private Channel getAndCreateNameserverChannel() throws InterruptedException {
        String addr = this.namesrvAddrChoosed.get();
        if (addr != null) {
            ChannelWrapper channelWrapper = this.channelTables.get(addr);
            if (channelWrapper != null && channelWrapper.isOK()) {
                return channelWrapper.getChannel();
            }
        }

        final List<String> addrList = this.namesrvAddrList.get();
        if (this.lockChannelTables.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
            try {
                addr = this.namesrvAddrChoosed.get();
                if (addr != null) {
                    ChannelWrapper channelWrapper = this.channelTables.get(addr);
                    if (channelWrapper != null && channelWrapper.isOK()) {
                        return channelWrapper.getChannel();
                    }
                }

                if (addrList != null && !addrList.isEmpty()) {
                    for (int i= 0; i < addrList.size(); i++) {
                        int index = this.namesrvIndex.incrementAndGet();
                        index = Math.abs(index);
                        index = index % addrList.size();
                        String newAddr = addrList.get(index);

                        this.namesrvAddrChoosed.set(newAddr);
                        Channel newChannel = this.createChannel(newAddr);
                        if (newChannel != null) {
                            return newChannel;
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("getAndCreatenameserverChannel : create name server channel exception",e);
            } finally {
                this.lockNamesrvChannel.unlock();
            }
        } else {
            logger.warn("getAndCreatenameServerChannel : try to local name server , but timeout, {} ms", LockTimeoutMillis);
        }

        return null;
    }

    private Channel createChannel(final String addr) throws  InterruptedException {

        ChannelWrapper channelWrapper = this.channelTables.get(addr);
        if (channelWrapper != null && channelWrapper.isOK()) {
            return channelWrapper.getChannel();
        }

        if (this.lockChannelTables.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
            try {
                boolean createNewConnection = false;
                channelWrapper = this.channelTables.get(addr);

                if (channelWrapper != null) {
                    if (channelWrapper.isOK()) {
                        return channelWrapper.getChannel();
                    } else if (!channelWrapper.getChannelFuture().isDone()) {
                        createNewConnection = false;
                    } else {
                        this.channelTables.remove(addr);
                        createNewConnection = true;
                    }
                } else {
                    createNewConnection = true;
                }

                if (createNewConnection) {
                    ChannelFuture channelFuture =
                            this.bootstrap.connect(RemotingHelper.string2SocketAddress(addr));
                    logger.info("createChannel : begin to connect remote host[{}] asynchronously", addr);
                    channelWrapper = new ChannelWrapper(channelFuture);
                    this.channelTables.put(addr, channelWrapper);
                }
            }  catch (Exception e) {
                logger.error("createChannel : create channel excpetion", e);
            } finally {
                this.lockChannelTables.unlock();
            }
        } else {
            logger.warn("createChannel : try to lock channel table , but timeout, {}ms", LockTimeoutMillis);
        }

        if (channelWrapper != null) {
            ChannelFuture channelFuture = channelWrapper.getChannelFuture();
            if (channelFuture.awaitUninterruptibly(this.nettyClientConfig.getConnectTimeoutMillis())) {
                if (channelWrapper.isOK()) {
                    logger.info("createChannel : connect remote host[{}] success, {}",
                            addr, channelFuture.toString());
                    return channelWrapper.getChannel();
                } else {
                    logger.warn("createChannel : connect host [" + addr + "] failed" +
                    channelFuture.toString(), channelFuture.cause());
                }
            }else {
                logger.warn("createChannel : connect remote host[{}] timeout {}ms, {}",
                        addr, this.nettyClientConfig.getConnectTimeoutMillis(), channelFuture.toString());
            }
        }

        return null;
    }

    public void closeChannel(final String addr, final Channel channel) {
        if (channel == null) {
            return;
        }

        final String remoteAddr = (addr == null ? RemotingHelper.parseChannelRemoteAddr(channel) : addr);

        try {
            if (this.lockChannelTables.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                   boolean removeItemFromTable = true;
                    final ChannelWrapper preChannelWrapper = this.channelTables.get(remoteAddr);

                    logger.info("closeChannel: begin close the channel[{}] Found: {}", remoteAddr, (preChannelWrapper != null));

                    if (preChannelWrapper == null) {
                        logger.info(
                                "closeChannel: the channel[{}] has been removed from the channel table before",
                                remoteAddr);
                        removeItemFromTable = false;
                    } else if (preChannelWrapper.getChannel() != channel) {
                        logger.info(
                                "closeChannel: the channel[{}] has been closed before, and has been created again, nothing to do.",
                                remoteAddr);
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        this.channelTables.remove(remoteAddr);
                        logger.info("closeChannel: the channel[{}] was removed from channel table", remoteAddr);
                    }

                    RemotingUtil.closeChannel(channel);
                } catch (Exception e)  {
                    logger.error("closeChannel : close the channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                logger.warn("closeChannel: try to lock channel table, but timeout, {}ms", LockTimeoutMillis);
            }
        } catch (InterruptedException e) {
            logger.error("closeChannel exception", e);
        }
    }

    public void closeChannel(final Channel channel) {
        if (null == channel)
            return;

        try {
            if (this.lockChannelTables.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    ChannelWrapper prevCW = null;
                    String addrRemote = null;
                    for (String key : channelTables.keySet()) {
                        ChannelWrapper prev = this.channelTables.get(key);
                        if (prev.getChannel() != null) {
                            if (prev.getChannel() == channel) {
                                prevCW = prev;
                                addrRemote = key;
                                break;
                            }
                        }
                    }

                    if (null == prevCW) {
                        logger.info(
                                "eventCloseChannel: the channel[{}] has been removed from the channel table before",
                                addrRemote);
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        this.channelTables.remove(addrRemote);
                        logger.info("closeChannel: the channel[{}] was removed from channel table", addrRemote);
                        RemotingUtil.closeChannel(channel);
                    }
                }
                catch (Exception e) {
                    logger.error("closeChannel: close the channel exception", e);
                }
                finally {
                    this.lockChannelTables.unlock();
                }
            }
            else {
                logger.warn("closeChannel: try to lock channel table, but timeout, {}ms", LockTimeoutMillis);
            }
        }
        catch (InterruptedException e) {
            logger.error("closeChannel exception", e);
        }
    }

    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        ExecutorService executorThis = executor;
        if (executor == null) {
            executorThis = this.publicExecutor;
        }

        Pair<NettyRequestProcessor, ExecutorService> pair =
                new Pair<NettyRequestProcessor, ExecutorService>(processor, executorThis);
        this.processTable.put(requestCode, pair);
    }

    @Override
    public RemotingCommand invokeSync(String addr, RemotingCommand request, long timeoutMillis) throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException {
        final Channel channel = this.getAndCreateChannel(addr);

        if (channel != null && channel.isActive()) {
            try {
                if (this.rpcHook != null) {
                    this.rpcHook.doBeforeRequest(addr, request);
                }

                RemotingCommand response = this.invokeSyncImpl(channel, request, timeoutMillis);

                if (this.rpcHook != null) {
                    this.rpcHook.doAfterResponse(RemotingHelper.parseChannelRemoteAddr(channel),
                            request, response);
                }
                return response;
            } catch (RemotingSendRequestException e) {
                logger.warn("invokeSync : send request exception, so close the channel [{}]", addr);
                this.closeChannel(addr, channel);
                throw  e;
            } catch (RemotingTimeoutException e) {
                logger.warn("invokeSync:wait response timeout exception, the channel [{}]", addr);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw  new RemotingConnectException(addr);
        }

    }

    @Override
    public void invokeAsync(String addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback) throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {

        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                if (this.rpcHook != null) {
                    this.rpcHook.doBeforeRequest(addr, request);
                }

                this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
            } catch (RemotingSendRequestException e) {
                logger.warn("invokeAsync : send request exception, so close the channel [{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    @Override
    public void invokeOneway(String addr, RemotingCommand request, long timeoutMillis) throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {

        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                if (this.rpcHook != null) {
                    this.rpcHook.doBeforeRequest(addr, request);
                }
                this.invokeOnewayImpl(channel, request, timeoutMillis);
            } catch (RemotingSendRequestException e) {
                logger.warn("invokeOneway: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return this.publicExecutor;
    }

    @Override
    public void updateNameServerddressList(List<String> addrs) {
        List<String> old = this.namesrvAddrList.get();
        boolean update = false;

        if (!addrs.isEmpty()) {
            if (old == null) {
                update = true;
            } else if (addrs.size() != old.size()) {
                update = true;
            } else {
                for (int i = 0; i < addrs.size() && !update; i++) {
                    if (!old.contains(addrs.get(i))) {
                        update = true;
                    }
                }
            }

            if (update) {
                Collections.shuffle(addrs);
                this.namesrvAddrList.set(addrs);
            }

        }
    }

    @Override
    public ChannelEventListener getChannelEventListener() {
        return channelEventListener;
    }

    @Override
    public List<String> getNameServerAddressList() {
        return this.namesrvAddrList.get();
    }

    @Override
    public RPCHook getRPCHook() {
        return rpcHook;
    }

    @Override
    public void registerRPCHook(RPCHook rpcHook) {
        this.rpcHook = rpcHook;
    }

    @Override
    public boolean isChannelWriteable(String addr) {
        ChannelWrapper channelWrapper = this.channelTables.get(addr);
        if (channelWrapper != null && channelWrapper.isOK()) {
            return channelWrapper.isWritable();
        }
        return true;
    }
}
