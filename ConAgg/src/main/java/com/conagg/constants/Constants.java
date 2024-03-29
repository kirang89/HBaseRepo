package com.conagg.constants;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * List of constants used throughout the application
 * 
 * @author <a HREF="mailto:kiran.daredevil@gmail.com">Kiran
 *         Gangadharan</a>
 * 
 */
@SuppressWarnings("javadoc")
public class Constants {
	/* HBase Configuration Keys */
	public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM =
			"hbase.zookeeper.quorum";
	public static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT =
			"hbase.zookeeper.property.clientPort";
	public static final String HBASE_CONFIGURATION_ZOOKEEPER_ZNODE_PARENT =
			"zookeeper.znode.parent";
	public static final String HBASE_CONFIGURATION_FILE = "";
	public static final String HBASE_MASTER = "hbase.master";
	public static final String ZOOKEEPER_ZNODE_PARENT_VALUE = "/hbase-0.92.1";

	/* Notations for HBase scan operations */
	public static final String PARTIAL_SCAN = "PS";
	public static final String SINGLE_COLUMN_SINGLE_VALUE_FILTER = "SCSVF";
	public static final String SINGLE_COLUMN_MULTIPLE_VALUE_FILTER = "SCMVF";
	public static final String GEQ = ">=";
	public static final String EQ = ">=";
	public static final String LT = "<";
	public static final String INTEGER = "int";
	public static final String LONG = "long";
	public static final String FLOAT = "float";
	public static final String DOUBLE = "double";
	public static final String STRING = "string";
	public static final String HBASE_ROW_FILTER = "ROW";
	public static final String HBASE_FAMILY_FILTER = "FAMILY";
	public static final String HBASE_QUALIFIER_FILTER = "QUALIFIER";
	public static final String HBASE_VALUE_FILTER = "VALUE";

	/* HBase datatype notations */
	public final static int INT_CELL = 1;
	public final static int LONG_CELL = 2;
	public final static int STRING_CELL = 3;
	public final static int DATE_CELL = 4;
	public final static int DOUBLE_CELL = 5;
	public final static int FLOAT_CELL = 6;
	public static final int STRING_ARRAY_CELL = 7;

	/* Directory Configurations */
	public static final String PROJECT_DIRECTORY = System
			.getProperty("user.dir");
	public static final String CONF_DIRECTORY = PROJECT_DIRECTORY
			+ "/src/main/resources/conf";
	public static final String LOGS_DIRECTORY = PROJECT_DIRECTORY
			+ "/src/main/resources/logs";

	/* Property File Configurations */
	public static final String SOURCE_SINK_PROPERTIES = CONF_DIRECTORY
			+ "/source-sink.properties";
	public static final String DATA_PROPERTIES = CONF_DIRECTORY
			+ "/data.properties";

	/* HBase Data Source Configurations */
	public static final String SOURCE_ZK_QUORUM_KEY =
			"source.hbase.zookeeper.quorum";
	public static final String SOURCE_ZK_CLIENTPORT_KEY =
			"source.hbase.zookeeper.clientPort";
	public static final String SOURCE_ZK_ZNODEPARENT_KEY =
			"source.hbase.zookeeper.znodeparent";
	public static final String SOURCE_HTABLE_KEY = "source.hbase.table.name";
	public static final String SOURCE_HCF_KEY = "source.hbase.table.cf";
	public static final String SOURCE_AUTOFLUSH_KEY =
			"source.hbase.property.autoflush";
	public static final String SOURCE_HBASE_RK_LENGTH_KEY =
			"source.hbase.property.rowkey.length";

	/* HBase Data Sink Configurations */
	public static final String SINK_ZK_QUORUM_KEY =
			"sink.hbase.zookeeper.quorum";
	public static final String SINK_ZK_CLIENTPORT_KEY =
			"sink.hbase.zookeeper.clientPort";
	public static final String SINK_ZK_ZNODEPARENT_KEY =
			"sink.hbase.zookeeper.znodeparent";
	public static final String SINK_HTABLE_KEY = "sink.hbase.table.name";
	public static final String SINK_HCF_KEY = "sink.hbase.table.cf";

	/* Logs Configurations */
	public static final String READER_LOG_DIRECTORY = LOGS_DIRECTORY
			+ "/reader.log";
	public static final String AGGREGATOR_LOG_DIRECTORY = LOGS_DIRECTORY
			+ "/aggregator.log";
	public static final String WRITER_LOG_DIRECTORY = LOGS_DIRECTORY
			+ "/writer.log";
	public static final String APPLICATION_LOG_DIR = LOGS_DIRECTORY
			+ "/application.log";
	public static final String SOURCE_SINK_LOG_DIR = LOGS_DIRECTORY
			+ "/source-sink.log";

	public static final int MAX_DATE_RANGE = 30;

	/* HBase Column Configurations */

	public static final String RTB_DATE = "date";
	public static final String RTB_SSP_ID = "ssp_id";
	public static final String RTB_CAMPAIGN_ID = "campaign_id";
	public static final String RTB_AD_ID = "ad_id";
	public static final String RTB_WIN = "win";
	public static final String RTB_LOSS = "loss";
	public static final String RTB_ACTUAL_BID = "actual_bid";
	public static final String RTB_DELIVERY_HOUR = "delivery_hour";
	public static final String RTB_DELIVERY_MIN = "delivery_min";
	public static final String RTB_DELIVERY_SEC = "delivery_sec";
	public static final String RTB_RESPONSE_TIME = "response_time";
	public static final String RTB_IMPRESSION_ID = "impression_id";
	public static final String COUNTER_CLICKS = "clicks";
	public static final String COUNTER_NOADS = "noads";
	public static final String COUNTER_CONVERSIONS = "conversions";

	/* Date format configuration used in application */
	public static final DateFormat outputDateFormat = new SimpleDateFormat(
			"yyyy-MM-dd");

}
