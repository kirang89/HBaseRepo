package com.conagg.app;

import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.conagg.constants.Constants;
import com.conagg.hbase.ds.Row;
import com.conagg.utils.ConAggUtils;

/**
 * Writer Thread to write data to HBase
 * 
 * @author kiran
 * 
 */
public class Writer implements Runnable {

	private static Logger logger;
	private static List<Row> sinkData;

	/**
	 * Initialise the member variables
	 * 
	 * @param input
	 *            data to be written to sink
	 */
	public void init(List<Row> input) {
		logger =
				ConAggUtils.createLogger("Writer",
						Constants.WRITER_LOG_DIRECTORY, Level.ALL);
		sinkData = input;
	}

	@Override
	public void run() {
		try {
			writeDataToSink();
		} catch (Exception e) {
			logger.error(e.toString());
			System.exit(1);
		}
	}

	private void writeDataToSink() {
		// HBase insertion logic
	}
}
