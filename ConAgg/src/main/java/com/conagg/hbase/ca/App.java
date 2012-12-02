package com.conagg.hbase.ca;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;

/**
 * The main class of Continuous Aggregator
 * 
 * @author <a HREF="mailto:kiran.daredevil@gmail.com">Kiran
 *         Gangadharan</a>
 * 
 * 
 */
public class App {
	@SuppressWarnings({ "javadoc", "static-access" })
	public static void main(String[] args) {
		Logger logger = Logger.getLogger("App");
		long start = System.currentTimeMillis();
		String date = "", hour = "";

		logger.info("Starting Continuous Aggregator");

		Options options = new Options();
		Option dateOption =
				OptionBuilder.withArgName("property=value").hasArgs(2)
						.withValueSeparator()
						.withDescription("Date in yyy-MM-dd").create("date");
		Option hourOption =
				OptionBuilder.withArgName("property=value").hasArgs(2)
						.withValueSeparator()
						.withDescription("Hour in 24 hr format").create("hour");
		Option helpOption =
				OptionBuilder.withDescription("Show usage").create("help");

		options.addOption(dateOption);
		options.addOption(hourOption);
		options.addOption(helpOption);

		HelpFormatter formatter = new HelpFormatter();
		CommandLineParser parser = new GnuParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
			if (cmd == null || cmd.hasOption("help")) {
				formatter.printHelp("help", options);
				System.exit(1);
			} else {
				if (cmd.hasOption("date") && cmd.hasOption("hour")) {
					hour = cmd.getOptionValue("hour");
					date = cmd.getOptionValue("date");
				} else if (cmd.hasOption("hour")) {
					System.out.println("Hour-wise");
					hour = cmd.getOptionValue("hour");
				} else if (cmd.hasOption("date")) {
					System.out.println("Day-wise");
					date = cmd.getOptionValue("date");
				}
			}

			logger.info("Date: " + date + " Hour: " + hour + " in "
					+ (System.currentTimeMillis() - start) + " ms");

			// Initialise and start Reader
			Reader reader = new Reader();
			reader.init();
			Thread readerThread = new Thread(reader);
			readerThread.start();

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
