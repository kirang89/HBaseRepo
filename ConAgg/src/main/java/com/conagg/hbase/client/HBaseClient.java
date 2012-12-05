package com.conagg.hbase.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.conagg.constants.Constants;
import com.conagg.hbase.ds.Field;
import com.conagg.hbase.ds.Row;
import com.conagg.hbase.utils.RowKeyGenerator;
import com.conagg.utils.ConAggUtils;

/**
 * HBase client wrapper
 * 
 * @author <a HREF="mailto:kiran.daredevil@gmail.com">Kiran
 *         Gangadharan</a>
 * 
 */
public class HBaseClient {

	private org.apache.hadoop.conf.Configuration conf = null;
	private HTable table;
	private HTablePool pool;
	private String zkClientPort = "";
	private String zkQuorum = "";
	private String zkZnodeParent = "";
	private Scan scan;
	private String tableName;
	private String columnFamily;
	private int rowKeyLength;
	private static List<Filter> filters = new ArrayList<Filter>();
	private static Configuration sourceConfig;
	private static Logger logger;

	/**
	 * Load HBase configuration from source-sink.properties
	 */
	public void loadConfiguration() {
		try {
			logger =
					ConAggUtils.createLogger("HBase",
							Constants.SOURCE_SINK_LOG_DIR, Level.ALL);
			sourceConfig =
					new PropertiesConfiguration(
							Constants.SOURCE_SINK_PROPERTIES);

			this.zkQuorum =
					sourceConfig.getProperty(Constants.SOURCE_ZK_QUORUM_KEY)
							.toString();
			this.zkClientPort =
					sourceConfig
							.getProperty(Constants.SOURCE_ZK_CLIENTPORT_KEY)
							.toString();
			this.zkZnodeParent =
					sourceConfig.getProperty(
							Constants.SOURCE_ZK_ZNODEPARENT_KEY).toString();
			this.tableName =
					sourceConfig.getProperty(Constants.SOURCE_HTABLE_KEY)
							.toString();
			this.columnFamily =
					sourceConfig.getProperty(Constants.SOURCE_HCF_KEY)
							.toString();
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Initialise HBase Connection
	 * 
	 * @throws IOException
	 */
	public void initConnection() throws IOException {
		conf = HBaseConfiguration.create();
		conf.set(Constants.HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, this.zkQuorum);
		conf.set(Constants.HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT,
				this.zkClientPort);
		conf.set(Constants.HBASE_CONFIGURATION_ZOOKEEPER_ZNODE_PARENT,
				this.zkZnodeParent);
		try {
			pool = new HTablePool(conf, 100);
			table = (HTable) pool.getTable(tableName);
			table.setAutoFlush(Boolean.parseBoolean(sourceConfig.getProperty(
					Constants.SOURCE_AUTOFLUSH_KEY).toString()));
		} catch (Exception e) {
			logger.error(e.getLocalizedMessage(), e);
		}
	}

	/**
	 * Scan for data with just filters and no row key scans.
	 * 
	 * @return instance of {@code ResultScanner}
	 * @throws IOException
	 */
	public ResultScanner scanWithFilter() throws IOException {

		Scan sc = new Scan();
		for (int i = 0; i < filters.size(); i++) {
			sc.setFilter(filters.get(i));
		}

		ResultScanner rs = table.getScanner(sc);

		return rs;
	}

	/**
	 * Set hbase filters based on the input given
	 * 
	 * @param filterList
	 *            list of qualifiers and their values
	 */
	@SuppressWarnings("rawtypes")
	public void addToFilter(Map<byte[], byte[]> filterList) {

		Iterator it = filterList.entrySet().iterator();

		while (it.hasNext()) {
			Map.Entry pairs = (Map.Entry) it.next();
			byte[] qualifier = (byte[]) pairs.getKey();
			byte[] value = (byte[]) pairs.getValue();
			Filter filter1 =
					new SingleColumnValueFilter(
							Bytes.toBytes(this.columnFamily), qualifier,
							CompareFilter.CompareOp.EQUAL, value);

			filters.add(filter1);
		}
	}

	/**
	 * Set hbase {@code ColumnValueFilter } based on the input given
	 * 
	 * @param qualifier
	 * @param comparator
	 * @param value
	 */
	public void setColumnValueFilter(byte[] qualifier, String comparator,
			byte[] value) {

		Filter filter1 = null;

		if (comparator.trim().equals("<"))
			filter1 =
					new SingleColumnValueFilter(
							Bytes.toBytes(this.columnFamily), qualifier,
							CompareFilter.CompareOp.LESS, value);
		else if (comparator.trim().equals("="))
			filter1 =
					new SingleColumnValueFilter(
							Bytes.toBytes(this.columnFamily), qualifier,
							CompareFilter.CompareOp.EQUAL, value);
		else if (comparator.trim().equals(">"))
			filter1 =
					new SingleColumnValueFilter(
							Bytes.toBytes(this.columnFamily), qualifier,
							CompareFilter.CompareOp.GREATER, value);
		else if (comparator.trim().equals("<="))
			filter1 =
					new SingleColumnValueFilter(
							Bytes.toBytes(this.columnFamily), qualifier,
							CompareFilter.CompareOp.GREATER_OR_EQUAL, value);
		else if (comparator.trim().equals(">="))
			filter1 =
					new SingleColumnValueFilter(
							Bytes.toBytes(this.columnFamily), qualifier,
							CompareFilter.CompareOp.LESS_OR_EQUAL, value);
		else
			filter1 =
					new SingleColumnValueFilter(
							Bytes.toBytes(this.columnFamily), qualifier,
							CompareFilter.CompareOp.NO_OP, value);

		filters.add(filter1);

	}

	/**
	 * @param filterType
	 * @param comparator
	 * @param searchParam
	 */
	public void setFilter(String filterType, String comparator,
			byte[] searchParam) {

		Filter tempFilter = null;
		/*
		 * if (filterType.trim().equals(Constants.HBASE_ROW_FILTER)) {
		 * // Row
		 * 
		 * if (comparator.trim().equals("<")) { tempFilter = new
		 * RowFilter(CompareFilter.CompareOp.LESS, new
		 * BinaryComparator(searchParam)); } else if
		 * (comparator.trim().equals(">")) { tempFilter = new
		 * RowFilter(CompareFilter.CompareOp.GREATER, new
		 * BinaryComparator(searchParam)); } else if
		 * (comparator.trim().equals("=")) { tempFilter = new
		 * RowFilter(CompareFilter.CompareOp.EQUAL, new
		 * BinaryComparator(searchParam)); } else if
		 * (comparator.trim().equals("<=")) { tempFilter = new
		 * RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new
		 * BinaryComparator(searchParam)); } else if
		 * (comparator.trim().equals(">=")) { tempFilter = new
		 * RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new
		 * BinaryComparator(searchParam)); } else { tempFilter = new
		 * RowFilter(CompareFilter.CompareOp.NO_OP, new
		 * BinaryComparator(searchParam)); }
		 * 
		 * } else
		 */if (filterType.trim().equals(Constants.HBASE_FAMILY_FILTER)) { // Family

			if (comparator.trim().equals("<")) {
				tempFilter =
						new FamilyFilter(CompareFilter.CompareOp.LESS,
								new BinaryComparator(searchParam));
			} else if (comparator.trim().equals(">")) {
				tempFilter =
						new FamilyFilter(CompareFilter.CompareOp.GREATER,
								new BinaryComparator(searchParam));
			} else if (comparator.trim().equals("=")) {
				tempFilter =
						new FamilyFilter(CompareFilter.CompareOp.EQUAL,
								new BinaryComparator(searchParam));
			} else if (comparator.trim().equals("<=")) {
				tempFilter =
						new FamilyFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
								new BinaryComparator(searchParam));
			} else if (comparator.trim().equals(">=")) {
				tempFilter =
						new FamilyFilter(
								CompareFilter.CompareOp.GREATER_OR_EQUAL,
								new BinaryComparator(searchParam));
			} else {
				tempFilter =
						new FamilyFilter(CompareFilter.CompareOp.NO_OP,
								new BinaryComparator(searchParam));
			}

		} else if (filterType.trim().equals(Constants.HBASE_QUALIFIER_FILTER)) { // Qualifier

			System.out.println("qualifier");
			if (comparator.trim().equals("<")) {
				tempFilter =
						new QualifierFilter(CompareFilter.CompareOp.LESS,
								new BinaryComparator(searchParam));
			} else if (comparator.trim().equals(">")) {
				tempFilter =
						new QualifierFilter(CompareFilter.CompareOp.GREATER,
								new BinaryComparator(searchParam));
			} else if (comparator.trim().equals("=")) {
				tempFilter =
						new QualifierFilter(CompareFilter.CompareOp.EQUAL,
								new BinaryComparator(searchParam));
			} else if (comparator.trim().equals("<=")) {
				tempFilter =
						new QualifierFilter(
								CompareFilter.CompareOp.LESS_OR_EQUAL,
								new BinaryComparator(searchParam));
			} else if (comparator.trim().equals(">=")) {
				tempFilter =
						new QualifierFilter(
								CompareFilter.CompareOp.GREATER_OR_EQUAL,
								new BinaryComparator(searchParam));
			} else {
				tempFilter =
						new QualifierFilter(CompareFilter.CompareOp.NO_OP,
								new BinaryComparator(searchParam));
			}
		} else if (filterType.trim().equals(Constants.HBASE_VALUE_FILTER)) { // Value

			if (comparator.trim().equals("<")) {
				tempFilter =
						new ValueFilter(CompareFilter.CompareOp.LESS,
								new BinaryComparator(searchParam));
			} else if (comparator.trim().equals(">")) {
				tempFilter =
						new ValueFilter(CompareFilter.CompareOp.GREATER,
								new BinaryComparator(searchParam));
			} else if (comparator.trim().equals("=")) {
				tempFilter =
						new ValueFilter(CompareFilter.CompareOp.EQUAL,
								new BinaryComparator(searchParam));
			} else if (comparator.trim().equals("<=")) {
				tempFilter =
						new ValueFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
								new BinaryComparator(searchParam));
			} else if (comparator.trim().equals(">=")) {
				tempFilter =
						new ValueFilter(
								CompareFilter.CompareOp.GREATER_OR_EQUAL,
								new BinaryComparator(searchParam));
			} else {
				tempFilter =
						new ValueFilter(CompareFilter.CompareOp.NO_OP,
								new BinaryComparator(searchParam));
			}
		}

		filters.add(tempFilter);
	}

	/**
	 * Partial Scans data based on start and end row provided
	 * 
	 * @param start
	 *            start row
	 * @param end
	 *            end row
	 * @return instance of {@code ResultScanner}
	 */
	public ResultScanner partialScan(byte[] start, byte[] end) {
		ResultScanner scanner = null;
		try {
			FilterList filterList = new FilterList(filters);
			scan = new Scan();
			if (filters != null)
				scan.setFilter(filterList);
			scan.setStartRow(start);
			scan.setStopRow(end);
			scan.setCaching(100);
			scanner = table.getScanner(scan);
		} catch (Exception e) {
			System.out.println("ERROR WHILE DOING PARTIAL SCAN");
			e.printStackTrace();
		}

		return scanner;
	}

	/**
	 * Function to increment a given input float value by 1
	 */
	private float incrementFloatValue(float f) {

		String temp = Float.toString(f);
		int integerPlaces = temp.indexOf('.');
		int decimalPlaces = temp.length() - integerPlaces - 1;
		float tempVal = (float) (1 / Math.pow(10, decimalPlaces));
		float newVal = f + tempVal;

		return newVal;
	}

	/**
	 * Function to increment a given input double value by 1
	 */
	private double incrementDoubleValue(double d) {

		String temp = Double.toString(d);
		int integerPlaces = temp.indexOf('.');
		int decimalPlaces = temp.length() - integerPlaces - 1;
		float tempVal = (float) (1 / Math.pow(10, decimalPlaces));
		double newVal = (double) (d + tempVal);

		return newVal;
	}

	public List<Row> getDataSet() {
		return null;
	}

	/**
	 * Set row key length
	 * 
	 * @param len
	 */
	public void setRowKeyLength(int len) {
		this.rowKeyLength = len;
	}

	/**
	 * @param inputData
	 * @param propFilePath
	 * @return ResultScanner
	 * @throws ParseException
	 * @throws IOException
	 */
	public ResultScanner getRecords(Map<String, Object> inputData,
			String propFilePath) throws ParseException, IOException {

		byte[] startKey = new byte[124];
		byte[] endKey = new byte[124];
		List<Object> temp = null;
		Map<String, List<Object>> startKeyData =
				new HashMap<String, List<Object>>();
		Map<String, List<Object>> endKeyData =
				new HashMap<String, List<Object>>();
		/*
		 * Getting row key configurations from the properties file
		 */
		Properties props = new Properties();
		FileInputStream fin =
				new FileInputStream(
						new File(Constants.HBASE_CONFIGURATION_FILE));
		props.load(fin);

		for (Map.Entry<String, Object> entry : inputData.entrySet()) {

			String key = (String) entry.getKey();
			String keyConfig = props.getProperty(key);
			String[] keyData = keyConfig.split(",");
			// The start position in the row key is
			// predefined for each field in
			// the key
			int startPosition = Integer.parseInt(keyData[0]);
			char[] type = keyData[1].toCharArray();
			temp = new ArrayList<Object>();
			/*
			 * Based on the type of the key(Integer,String etc...), we
			 * provide different implementations for both start and
			 * end key
			 */
			switch (type[0]) {
				case 'i': // Integer
					int intValue = (Integer) inputData.get(key);
					temp.add(0, intValue); // Setting value
											// to list
					temp.add(1, startPosition); // Setting
												// position
					temp.add(2, 1); // Setting type of value
					startKeyData.put(key, temp);
					temp = new ArrayList<Object>();
					temp.add(0, ++intValue);
					temp.add(1, startPosition);
					temp.add(2, 1);
					endKeyData.put(key, temp);
					break;
				case 'l': // Long
					long longValue = (Long) inputData.get(key);
					temp.add(0, longValue);
					temp.add(1, startPosition);
					temp.add(2, 2);
					startKeyData.put(key, temp);
					temp = new ArrayList<Object>();
					temp.add(0, ++longValue);
					temp.add(1, startPosition);
					temp.add(2, 2);
					endKeyData.put(key, temp);
					break;
				case 's': // String
					String stringValue = (String) inputData.get(key);
					temp.add(0, stringValue);
					temp.add(1, startPosition);
					temp.add(2, 3);
					startKeyData.put(key, temp);
					// Converting String to char array for
					// incrementing and then
					// storing it back into a String
					char[] tempArray = stringValue.toCharArray();
					++tempArray[tempArray.length - 1];
					stringValue = new String(tempArray);
					temp = new ArrayList<Object>();
					temp.add(0, stringValue);
					temp.add(1, startPosition);
					temp.add(2, 3);
					endKeyData.put(key, temp);
					break;
				case 't': // Date
					String dateValue = (String) inputData.get(key);
					List<Object> startList = new ArrayList<Object>();
					startList.add(0, dateValue);
					startList.add(1, startPosition);
					startList.add(2, 4);
					startKeyData.put(key, startList);
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
					Calendar c = Calendar.getInstance();
					c.setTime(sdf.parse(dateValue));
					c.add(Calendar.DATE, 1); // number of
												// days to
												// add
					String incrementDateValue = sdf.format(c.getTime());
					;
					List<Object> endList = new ArrayList<Object>();
					endList.add(0, incrementDateValue);
					endList.add(1, startPosition);
					endList.add(2, 4); // Need to add type
										// data for double
					endKeyData.put(key, endList);
					break;
				case 'd': // Double
					double doubleValue = (Double) inputData.get(key);
					temp.add(0, doubleValue);
					temp.add(1, startPosition);
					temp.add(2, 5);
					startKeyData.put(key, temp);
					double incrementedDoubleValue =
							incrementDoubleValue(doubleValue);
					temp.add(0, incrementedDoubleValue);
					temp.add(1, startPosition);
					temp.add(2, 5); // Need to add type data
									// for double
					endKeyData.put(key, temp);
					break;
				case 'f': // Float
					float floatValue = (Float) inputData.get(key);
					temp.add(0, floatValue);
					temp.add(1, startPosition);
					temp.add(2, 6);
					startKeyData.put(key, temp);
					double incrementedFloatValue =
							incrementFloatValue(floatValue);
					temp.add(0, incrementedFloatValue);
					temp.add(1, startPosition);
					temp.add(2, 6); // Need to add type data
									// for float
					endKeyData.put(key, temp);
					break;
				default:
					System.out.println("Invalid type detected");
					break;
			}
		}

		/*
		 * Getting the full rowKey in bytes based on the start and end
		 * key provided
		 */
		RowKeyGenerator bg = new RowKeyGenerator();
		startKey = bg.getKey(startKeyData);
		endKey = bg.getKey(endKeyData);

		ResultScanner resultSet = partialScan(startKey, endKey);

		return resultSet;
	}

	/**
	 * Evaluate data provided, and decide which ones to apply for
	 * partial scan and which ones as filters for getting required
	 * data from HBase
	 * 
	 * @param fields
	 *            list of type {@code Field}
	 * @return instance of {@code ResultScanner}
	 * @throws ParseException
	 */
	@SuppressWarnings("unused")
	public ResultScanner scan(List<Field> fields) throws ParseException {

		Map<String, List<Object>> start = new HashMap<String, List<Object>>();
		Map<String, List<Object>> end = new HashMap<String, List<Object>>();
		boolean isError = false;

		for (int i = 0; i < fields.size(); i++) {
			Field f = fields.get(i);

			if (f.fieldAction.equalsIgnoreCase(Constants.PARTIAL_SCAN)) {
				if (f.fieldStartValue == null || f.fieldEndValue == null) {
					System.out.println("Start/end value cannot be null");
					isError = true;
					break;
				} else {
					List<Object> temp = new ArrayList<Object>();
					if (f.fieldType.equalsIgnoreCase(Constants.INTEGER)) {
						temp.add(0, (Integer) f.fieldStartValue);
						temp.add(1, f.fieldRowKeyPosition);
						temp.add(2, Constants.INT_CELL);
						start.put(f.fieldName, temp);

						temp = new ArrayList<Object>();
						temp.add(0, (Integer) f.fieldEndValue);
						temp.add(1, f.fieldRowKeyPosition);
						temp.add(2, Constants.INT_CELL);
						end.put(f.fieldName, temp);

					} else if (f.fieldType.equalsIgnoreCase(Constants.LONG)) {
						temp.add(0, (Long) f.fieldStartValue);
						temp.add(1, f.fieldRowKeyPosition);
						temp.add(2, Constants.LONG_CELL);
						start.put(f.fieldName, temp);

						temp = new ArrayList<Object>();
						temp.add(0, (Long) f.fieldEndValue);
						temp.add(1, f.fieldRowKeyPosition);
						temp.add(2, Constants.LONG_CELL);
						end.put(f.fieldName, temp);

					} else if (f.fieldType.equalsIgnoreCase(Constants.FLOAT)) {
						temp.add(0, (Float) f.fieldStartValue);
						temp.add(1, f.fieldRowKeyPosition);
						temp.add(2, Constants.FLOAT_CELL);
						start.put(f.fieldName, temp);

						temp = new ArrayList<Object>();
						temp.add(0, (Float) f.fieldEndValue);
						temp.add(1, f.fieldRowKeyPosition);
						temp.add(2, Constants.FLOAT_CELL);
						end.put(f.fieldName, temp);

					} else if (f.fieldType.equalsIgnoreCase(Constants.DOUBLE)) {
						temp.add(0, (Double) f.fieldStartValue);
						temp.add(1, f.fieldRowKeyPosition);
						temp.add(2, Constants.DOUBLE_CELL);
						start.put(f.fieldName, temp);

						temp = new ArrayList<Object>();
						temp.add(0, (Double) f.fieldEndValue);
						temp.add(1, f.fieldRowKeyPosition);
						temp.add(2, Constants.DOUBLE_CELL);
						end.put(f.fieldName, temp);

					} else if (f.fieldType.equalsIgnoreCase(Constants.STRING)) {
						temp.add(0, (String) f.fieldStartValue);
						temp.add(1, f.fieldRowKeyPosition);
						temp.add(2, Constants.STRING_CELL);
						start.put(f.fieldName, temp);

						temp = new ArrayList<Object>();
						temp.add(0, (String) f.fieldEndValue);
						temp.add(1, f.fieldRowKeyPosition);
						temp.add(2, Constants.STRING_CELL);
						end.put(f.fieldName, temp);
					}
				}

			} else if (f.fieldAction
					.equalsIgnoreCase(Constants.SINGLE_COLUMN_SINGLE_VALUE_FILTER)) {
				if (f.fieldOperator1 != null) {
					if (f.fieldType.equalsIgnoreCase(Constants.INTEGER)) {
						setColumnValueFilter(Bytes.toBytes(f.fieldName),
								f.fieldOperator1,
								Bytes.toBytes((Integer) f.fieldStartValue));
					} else if (f.fieldType.equalsIgnoreCase(Constants.LONG)) {
						setColumnValueFilter(Bytes.toBytes(f.fieldName),
								f.fieldOperator1,
								Bytes.toBytes((Long) f.fieldStartValue));
					} else if (f.fieldType.equalsIgnoreCase(Constants.FLOAT)) {
						setColumnValueFilter(Bytes.toBytes(f.fieldName),
								f.fieldOperator1,
								Bytes.toBytes((Float) f.fieldStartValue));
					} else if (f.fieldType.equalsIgnoreCase(Constants.DOUBLE)) {
						setColumnValueFilter(Bytes.toBytes(f.fieldName),
								f.fieldOperator1,
								Bytes.toBytes((Double) f.fieldStartValue));
					} else if (f.fieldType.equalsIgnoreCase(Constants.STRING)) {
						setColumnValueFilter(Bytes.toBytes(f.fieldName),
								f.fieldOperator1,
								Bytes.toBytes((String) f.fieldStartValue));
					}
				} else {
					isError = true;
					break;
				}

			} else if (f.fieldAction
					.equalsIgnoreCase(Constants.SINGLE_COLUMN_MULTIPLE_VALUE_FILTER)) {
				if (f.fieldOperator1 != null && f.fieldOperator2 != null
						|| f.fieldStartValue != null && f.fieldEndValue != null) {

					if (f.fieldType.equalsIgnoreCase(Constants.INTEGER)) {

						setColumnValueFilter(Bytes.toBytes(f.fieldName),
								f.fieldOperator1,
								Bytes.toBytes((Integer) f.fieldStartValue));
						setColumnValueFilter(Bytes.toBytes(f.fieldName),
								f.fieldOperator2,
								Bytes.toBytes((Integer) f.fieldEndValue));

					} else if (f.fieldType.equalsIgnoreCase(Constants.LONG)) {

						setColumnValueFilter(Bytes.toBytes(f.fieldName),
								f.fieldOperator1,
								Bytes.toBytes((Long) f.fieldStartValue));
						setColumnValueFilter(Bytes.toBytes(f.fieldName),
								f.fieldOperator2,
								Bytes.toBytes((Long) f.fieldEndValue));

					} else if (f.fieldType.equalsIgnoreCase(Constants.FLOAT)) {

						setColumnValueFilter(Bytes.toBytes(f.fieldName),
								f.fieldOperator1,
								Bytes.toBytes((Float) f.fieldStartValue));
						setColumnValueFilter(Bytes.toBytes(f.fieldName),
								f.fieldOperator2,
								Bytes.toBytes((Float) f.fieldEndValue));

					} else if (f.fieldType.equalsIgnoreCase(Constants.DOUBLE)) {

						setColumnValueFilter(Bytes.toBytes(f.fieldName),
								f.fieldOperator1,
								Bytes.toBytes((Double) f.fieldStartValue));
						setColumnValueFilter(Bytes.toBytes(f.fieldName),
								f.fieldOperator2,
								Bytes.toBytes((Double) f.fieldEndValue));

					} else if (f.fieldType.equalsIgnoreCase(Constants.STRING)) {

						setColumnValueFilter(Bytes.toBytes(f.fieldName),
								f.fieldOperator1,
								Bytes.toBytes((String) f.fieldStartValue));
						setColumnValueFilter(Bytes.toBytes(f.fieldName),
								f.fieldOperator2,
								Bytes.toBytes((String) f.fieldEndValue));

					}
				}
			}
		}

		RowKeyGenerator rowKeyGen = new RowKeyGenerator();
		rowKeyGen.setRowKeyLength(this.rowKeyLength);
		byte[] startRow = rowKeyGen.getKey(start);

		rowKeyGen = new RowKeyGenerator();
		rowKeyGen.setRowKeyLength(this.rowKeyLength);
		byte[] endRow = rowKeyGen.getKey(end);

		ResultScanner scannerInstance = partialScan(startRow, endRow);

		return scannerInstance;
	}

	/**
	 * Close HBase connection
	 * 
	 * @throws IOException
	 */
	public void closeConnection() throws IOException {
		if (table != null)
			table.close();
		conf.clear();
		if (pool != null)
			pool.close();
	}
}
