package com.conagg.hbase.ca;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.conagg.constants.Constants;

/**
 * Reader Thread to read data from HBase
 * 
 * @author <a HREF="mailto:kiran.daredevil@gmail.com">Kiran
 *         Gangadharan</a>
 * 
 */
public class Reader implements Runnable {

	private static String zkQuorum = null;
	private static String zkClientPort = null;
	private static String zkZnodeParent = null;
	private static String table = null;
	private static String columnFamily = null;
	private static boolean failed = false;
	private static Logger logger = Logger.getLogger("Reader");
	private static String date = null;
	private static String hour = null;

	@Override
	public void run() {
		if (failed != true)
			readFromSource();
		else
			logger.error("Reader failed");
	}

	/**
	 * Initialise the member variables
	 * 
	 * @throws ConfigurationException
	 */
	public static void init() throws ConfigurationException {
		failed = true;
		try {
			createLogger();
			Configuration datasourceConf =
					new PropertiesConfiguration(
							Constants.SOURCE_SINK_PROPERTIES);

			if (datasourceConf != null) {
				zkQuorum =
						datasourceConf.getProperty(
								Constants.SOURCE_ZK_QUORUM_KEY).toString();
				zkClientPort =
						datasourceConf.getProperty(
								Constants.SOURCE_ZK_QUORUM_KEY).toString();

				zkZnodeParent =
						datasourceConf.getProperty(
								Constants.SOURCE_ZK_QUORUM_KEY).toString();
				table =
						datasourceConf.getProperty(
								Constants.SOURCE_ZK_QUORUM_KEY).toString();
				columnFamily =
						datasourceConf.getProperty(
								Constants.SOURCE_ZK_QUORUM_KEY).toString();
				failed = false;
				logger.info("Initialized Successfully");
			}
		} catch (Exception e) {
			logger.error(e.toString());
			failed = true;
		}
	}

	private static void createLogger() {
		FileAppender fa = new FileAppender();
		fa.setName("Reader");
		fa.setFile("/home/kiran/workspace/ExampleStuff/logs/reader.log");
		fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
		fa.setThreshold(Level.ALL);
		fa.setAppend(true);
		fa.activateOptions();
		logger.addAppender(fa);
	}

	/**
	 * Read data from datasource
	 */
	public void readFromSource() {

	}

	/**
	 * Set the date in yyyy-MM-dd
	 * 
	 * @param dateString
	 *            date
	 */
	public static void setDate(String dateString) {
		date = dateString;
	}

	/**
	 * Set hour in 24 hr format
	 * 
	 * @param HourString
	 *            hour
	 */
	public static void setHour(String HourString) {
		hour = HourString;
	}

	/**
	 * Returns whether any failure has occured while running the
	 * thread
	 * 
	 * @return boolean true if failed else false
	 */
	public boolean hasFailed() {
		return failed;
	}
}
