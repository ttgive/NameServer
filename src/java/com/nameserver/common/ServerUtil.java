package com.nameserver.common;

import org.apache.commons.cli.*;

import java.util.Properties;

public class ServerUtil {

    public static Options buildCommandLineOptions(final Options options) {
        Option opt = new Option("h", "help", false, "Print help");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("n", "namesrvAddr", true,
                "Name server address list, eg : 192.168.0.1:9876; 192.168.0.2:9876");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    public static CommandLine parseCmdLine(final String appName, String[] args,
                                           Options options, CommandLineParser parser) {
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);
            if (commandLine.hasOption('h')) {
                hf.printHelp(appName, options, true);
                return null;
            }
        } catch (ParseException e) {
            hf.printHelp(appName, options, true);
        }

        return commandLine;
    }

    public static void printCommandLineHelp(final String appName,
                                            final Options options) {
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        hf.printHelp(appName, options, true);
    }

    public static Properties commandLine2Properties(final CommandLine commandLine) {
        Properties properties = new Properties();
        Option[] options = commandLine.getOptions();

        if (options != null) {
            for (Option opt : options) {
                String name = opt.getLongOpt();
                String value = commandLine.getOptionValue(name);
                if (value != null) {
                    properties.setProperty(name, value);
                }
            }
        }

        return properties;
    }
}
