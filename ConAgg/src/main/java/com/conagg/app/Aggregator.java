package com.conagg.app;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.conagg.constants.Constants;
import com.conagg.hbase.ds.Row;
import com.conagg.utils.ConAggUtils;

/**
 * Aggregator Thread to aggregate data, read from HBase
 * 
 * @author <a HREF="mailto:kiran.daredevil@gmail.com">Kiran
 *         Gangadharan</a>
 * 
 */
public class Aggregator implements Runnable {

	private static Logger logger;
	private static List<Row> data;

	/**
	 * Initialise the member variables
	 */
	public void init(List<Row> input) {
		logger =
				ConAggUtils.createLogger("Aggregator",
						Constants.AGGREGATOR_LOG_DIRECTORY, Level.ALL);
		data = input;
	}

	@Override
	public void run() {
		try {
			List<Row> aggregatedData = aggregate();
			startWriter(aggregatedData);
		} catch (Exception e) {
			logger.error(e.toString());
			System.exit(1);
		}
	}

	private List<Row> aggregate() {
		// Aggregation Logic
		return null;
	}

	private void startWriter(List<Row> input) {
		Writer writer = new Writer();
		writer.init(input);
		ExecutorService pool = Executors.newCachedThreadPool();
		pool.submit(writer);
	}

}
