package com.nameserver;

import com.nameserver.kvconfig.KVConfigManager;
import com.nameserver.processor.DefaultRequestProcessor;
import com.nameserver.remoting.RemotingServer;
import com.nameserver.remoting.netty.NettyRemotingServer;
import com.nameserver.remoting.netty.NettyServerConfig;
import com.nameserver.routeinfo.RouteInfoManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class NamesrvController {

    private static final Logger log = LoggerFactory.getLogger(NamesrvController.class);

    private final NamesrvConfig namesrvConfig;

    private final NettyServerConfig nettyServerConfig;

    private RemotingServer remotingServer;

    private BrokderHouseKeepingService brokderHouseKeepingService;

    private ExecutorService remotingExecutor;

    private final ScheduledExecutorService scheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("NSScheduledThread"));

    private final KVConfigManager kvConfigManager;
    private final RouteInfoManager routeInfoManager;

    public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig) {
        this.namesrvConfig = namesrvConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.kvConfigManager = new KVConfigManager(this);
        this.routeInfoManager = new RouteInfoManager();
        this.brokderHouseKeepingService = new BrokderHouseKeeping(this);
    }

    public boolean initialize() {
        this.kvConfigManager.load();

        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.brokderHouseKeepingService);

        this.remotingExecutor = Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(),
                new ThreadFactoryImpl("RemotingExecutorThread_"));

        this.registerProcessor();

        this.scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                NamesrvController.this.routeInfoManager.scanNotActiveBroker();
            }
        }, 5, 10 , TimeUnit.SECONDS);

        this.scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                NamesrvController.this.kvConfigManager.printAllPeriodically();
            }
        }, 1, 10, TimeUnit.MILLISECONDS);

        return true;
    }

    private void registerProcessor() {
        this.remotingServer.registerDefaultProcessor(new DefaultRequestProcessor(this),
                this.remotingExecutor);
    }

    public void start() {
        this.remotingServer.start();
    }

    public NamesrvConfig getNamesrvConfig() {
        return namesrvConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public KVConfigManager getKvConfigManager() {
        return kvConfigManager;
    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public void setRemotingExecutor(ExecutorService remotingExecutor) {
        this.remotingExecutor = remotingExecutor;
    }

    public RouteInfoManager getRouteInfoManager() {
        return routeInfoManager;
    }
}
