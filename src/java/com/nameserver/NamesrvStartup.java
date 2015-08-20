package com.nameserver;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import com.nameserver.common.NamesrvUtil;
import com.nameserver.common.ServerUtil;
import com.nameserver.remoting.netty.NettyServerConfig;
import com.nameserver.remoting.netty.NettySystemConfig;
import com.nameserver.remoting.protocol.RemotingCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.lang.model.element.Name;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class NamesrvStartup {

    public static Properties properties = null;
    public static CommandLine commandLine = null;

    public static Options buildCommandLineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    public static void main(String[] args) {
        main0(args);
    }

    public static NamesrvController main0(String[] args) {
        System.setProperty(RemotingCommand.RemotingVersionKey, Integer.toString(NameServerVersion.CurrentVersion));

        if (System.getProperty(NettySystemConfig.SystemPropertySocketSndBufSize) == null) {
            NettySystemConfig.SocketSndBufSize = 2048;
        }

        if (System.getProperty(NettySystemConfig.SystemPropertySocketRcvBufSize) == null) {
            NettySystemConfig.SocketRcvbufSize = 1024;
        }

        try {
            Options options = ServerUtil.buildCommandLineOptions(new Options());
            commandLine = ServerUtil.parseCmdLine("nameserv", args, buildCommandLineOptions(options),
                    new PosixParser());

            if (commandLine == null) {
                System.exit(-1);
                return null;
            }

            final NamesrvConfig namesrvConfig = new NamesrvConfig();
            final NettyServerConfig nettyServerConfig = new NettyServerConfig();
            nettyServerConfig.setListenPort(9876);

            if (commandLine.hasOption("c")) {
                String file = commandLine.getOptionValue("c");
                if (file != null) {
                    InputStream in = new BufferedInputStream(new FileInputStream(file));
                    properties = new Properties();
                    properties.load(in);

                    NamesrvUtil.properties2Object(properties, namesrvConfig);
                    NamesrvUtil.properties2Object(properties, nettyServerConfig);
                    System.out.println("load config properties file OK, " + file);
                    in.close();
                }
            }

            if (commandLine.hasOption('p')) {
                NamesrvUtil.printObjectProperties(null, namesrvConfig);
                NamesrvUtil.printObjectProperties(null, nettyServerConfig);
                System.exit(0);
            }

            NamesrvUtil.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);

            if (namesrvConfig.getRocketmqHome() == null) {
                System.out.println("please set rocketmq  vairable in your evinroment");
                System.exit(-2);
            }

            // 初始化Logback
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            configurator.doConfigure(namesrvConfig.getRocketmqHome() + "/conf/logback_namesrv.xml");
            final Logger log = LoggerFactory.getLogger(NamesrvStartup.class);

            NamesrvUtil.printObjectProperties(log, namesrvConfig);
            NamesrvUtil.printObjectProperties(log, nettyServerConfig);

            final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig);
            boolean initResult = controller.initialize();
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }

            Runtime.getRuntime().addShutdownHook(new Thread(
                    new Runnable() {
                        private volatile boolean hasShutdown = false;
                        private AtomicInteger shutdownTimes = new AtomicInteger(0);

                        @Override
                        public void run() {
                            synchronized (this) {
                                log.info("shutdown hook was invoked, " + this.shutdownTimes.incrementAndGet());
                                if (!this.hasShutdown) {
                                    this.hasShutdown = true;
                                    long beginTime = System.currentTimeMillis();
                                    controller.shutdown();
                                    long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                                    log.info("shutdown hook over, consuming time total(ms)" + consumingTimeTotal);
                                }
                            }
                        }
                    }, "ShutdownHook"));

            controller.start();
            String tip = "The name server boot success";
            log.info(tip);
            System.out.println(tip);

            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }


}
