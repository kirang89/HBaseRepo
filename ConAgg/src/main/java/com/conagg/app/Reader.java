package com.conagg.app;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.conagg.constants.Constants;
import com.conagg.hbase.client.HBaseClient;
import com.conagg.hbase.ds.Row;

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
	private static Configuration datasourceConf;

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
			logger.info("Initialising Reader");
			datasourceConf =
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
			logger.error(e.getLocalizedMessage(), e);
			failed = true;
		}
	}

	private static void createLogger() {
		FileAppender fa = new FileAppender();
		fa.setName("Reader");
		fa.setFile(Constants.READER_LOG_DIRECTORY);
		fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
		fa.setThreshold(Level.ALL);
		fa.setAppend(false);
		fa.activateOptions();
		logger.addAppender(fa);
	}

	/**
	 * Read data from datasource
	 */
	public void readFromSource() {
		try {
			if (date != null && !date.isEmpty())
				if (hour != null && !hour.isEmpty())
					readHourData();
				else
					readDayData();
			else
				throw new Exception("Date and hour cannot be null");

		} catch (Exception e) {
			logger.error(e.getLocalizedMessage(), e);
		}
	}

	public void readHourData() {
		logger.info("Reading data for hour: " + hour);
		long start = System.currentTimeMillis();
		int hourValue = Integer.parseInt(hour);
		Map<byte[], byte[]> filterList = new HashMap<byte[], byte[]>();

		try {
			filterList.put(Bytes.toBytes(Constants.RTB_DELIVERY_HOUR),
					Bytes.toBytes(hourValue));

			logger.info("SCANNING DATA FROM MASTER FOR HOUR: " + hour);

			HBaseClient hbaseScanner = new HBaseClient();
			hbaseScanner.loadConfiguration();
			hbaseScanner.initConnection();
			hbaseScanner.addToFilter(filterList);
			List<Row> sourceData = hbaseScanner.getDataSet();
			// ResultScanner scanner =
			// hbaseScanner.getData(
			// ConAggUtils.getLongDate(date),
			// date,
			// datasourceConf.getProperty(
			// Constants.SOURCE_HBASE_RK_LENGTH_KEY)
			// .toString());
		} catch (Exception e) {
			logger.error(e.getLocalizedMessage(), e);
			System.exit(1);
		}

		logger.info("Time taken to scan data: "
				+ (System.currentTimeMillis() - start));
	}

	public void readDayData() {
		// TODO Auto-generated method stub
		logger.info("Reading data for date: " + date);
	}

	public void startAggregation(List<Row> data) {
		try {
			Aggregator aggregator = new Aggregator();
			aggregator.init(data);
			ExecutorService pool = Executors.newCachedThreadPool();
			pool.submit(aggregator);
		} catch (Exception e) {
			logger.error(e.getLocalizedMessage(), e);
			System.exit(1);
		}
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
