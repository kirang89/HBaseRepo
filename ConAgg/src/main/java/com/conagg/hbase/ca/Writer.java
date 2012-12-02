package com.conagg.hbase.ca;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.conagg.constants.Constants;

/**
 * Writer Thread to write data to HBase
 * 
 * @author kiran
 * 
 */
public class Writer implements Runnable {

	private static Logger logger = Logger.getLogger("Writer");

	@Override
	public void run() {

	}

	public void init() {

	}

	/**
	 * Initialise logger
	 */
	public void createLogger() {
		FileAppender fa = new FileAppender();
		fa.setName("Writer");
		fa.setFile(Constants.WRITER_LOG_DIRECTORY);
		fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
		fa.setThreshold(Level.ALL);
		fa.setAppend(true);
		fa.activateOptions();
		logger.addAppender(fa);
	}
}
