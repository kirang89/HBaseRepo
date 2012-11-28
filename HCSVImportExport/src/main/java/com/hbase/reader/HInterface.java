package com.hbase.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.hbase.dataStructs.ConnectionConf;
import com.hbase.dataStructs.Constants;
import com.hbase.dataStructs.TableConf;

/**
 * HBase Read and Write operation interface
 * 
 * @author Kiran Gangadharan @ Komli Mobile
 * 
 */
public class HInterface {
	private static Configuration conf;
	private static HTable table;
	private static HTablePool pool;
	private static Scan scan;
	private static List<Filter> filters = new ArrayList<Filter>();
	private Put put;

	private Logger logger = Logger.getLogger(HInterface.class);

	private String host = "";
	private String zcport = "";
	private String znode = "";

	static {
		conf = HBaseConfiguration.create();
	}

	/**
	 * Initialise connection with HBase
	 * 
	 * @param host
	 * @param port
	 * @param tableName
	 * @param family
	 * @throws IOException
	 */
	/*
	 * public void init(String host, String port, String tableName,
	 * String family) throws IOException { zcport = port; this.znode =
	 * znodeParent;
	 * 
	 * // System.out.println("Host : " + host + " port : " // + port);
	 * // conf.set(Constants.HBASE_MASTER_PARAM, host + ":" + port);
	 * conf.set(Constants.ZOOKEEPER_QUORUM, host);
	 * conf.set(Constants.ZOOKEEPER_PORT, zcport);
	 * conf.set(Constants.ZOOKEEPER_ZNODE_PARENT, znode);
	 * 
	 * try { // GETTING NEW HTble INSTANCE FROM TABLE POOL // pool =
	 * new HTablePool(conf, 100);
	 * 
	 * HBaseAdmin admin = new HBaseAdmin(conf); if
	 * (admin.tableExists(tableName) == false) {
	 * logger.info("Creating table " + tableName); HTableDescriptor
	 * desc = new HTableDescriptor(Bytes.toBytes(tableName));
	 * HColumnDescriptor columnFamilyDef = new
	 * HColumnDescriptor(Bytes.toBytes(family));
	 * desc.addFamily(columnFamilyDef); admin.createTable(desc); } //
	 * table = new HTable(conf, tableName); table = (HTable)
	 * pool.getTable(tableName);
	 * logger.info("Writing to WAL enabled"); // WRITE TO WAL
	 * table.setAutoFlush(false);
	 * logger.info("Connection initialized"); } catch (Exception e) {
	 * logger.error("Error while initialising a HBase connection");
	 * e.printStackTrace(); } }
	 */
	public void init(ConnectionConf cconf, TableConf tconf) throws IOException {

		String tableName = tconf.getName();

		conf.set(Constants.ZOOKEEPER_QUORUM, cconf.getZkQuorum());
		conf.set(Constants.ZOOKEEPER_PORT, cconf.getZkClientPort());
		conf.set(Constants.ZOOKEEPER_ZNODE_PARENT, cconf.getZkZnodeParent());

		try {
			// GETTING NEW HTble INSTANCE FROM TABLE POOL
			// pool = new HTablePool(conf, 100);

			HBaseAdmin admin = new HBaseAdmin(conf);
			if (admin.tableExists(tableName) == false) {
				logger.info("Creating table " + tableName);
				HTableDescriptor desc =
						new HTableDescriptor(Bytes.toBytes(tableName));
				HColumnDescriptor columnFamilyDef =
						new HColumnDescriptor(Bytes.toBytes(tconf.getFamily()));
				desc.addFamily(columnFamilyDef);
				admin.createTable(desc);
			}
			// table = new HTable(conf, tableName);
			table = (HTable) pool.getTable(tableName);
			logger.info("Writing to WAL enabled");
			// WRITE TO WAL
			table.setAutoFlush(false);
			logger.info("Connection initialized");
		} catch (Exception e) {
			logger.error("Error while initialising a HBase connection");
			e.printStackTrace();
		}
	}

	/**
	 * Close HBase connection and release resources
	 * 
	 * @throws IOException
	 */
	public void closeConnection() throws IOException {
		logger.info("Closing Connection");
		HConnectionManager.deleteConnection(conf, true);
		table.close();
		pool.close();
	}

	/**
	 * Add row key to {@code Put}
	 * 
	 * @param key
	 */
	public void addRowKey(byte[] key) {
		put = new Put(key);
	}

	/*
	 * All Write Data instances
	 */

	public void addData(String family, String column, String value) {

		put.add(Bytes.toBytes(family), Bytes.toBytes(column),
				Bytes.toBytes(value));
	}

	public void addIntDataToWriter(String family, String column, int value) {

		put.add(Bytes.toBytes(family), Bytes.toBytes(column),
				Bytes.toBytes(value));
	}

	public void addStringDataToWriter(String family, String column, String value) {

		put.add(Bytes.toBytes(family), Bytes.toBytes(column),
				Bytes.toBytes(value));
	}

	public void addLongDataToWriter(String family, String column, long value) {

		put.add(Bytes.toBytes(family), Bytes.toBytes(column),
				Bytes.toBytes(value));
	}

	public void addDoubleDataToWriter(String family, String column, double value) {

		put.add(Bytes.toBytes(family), Bytes.toBytes(column),
				Bytes.toBytes(value));
	}

	public void addFloatDataToWriter(String family, String column, float value) {

		put.add(Bytes.toBytes(family), Bytes.toBytes(column),
				Bytes.toBytes(value));
	}

	public void writeDataToTableBuffer() {
		try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void writeDataToBuffer(byte[] key, String family, String column,
			String value) throws IOException {

		Put put1 = new Put(key);
		put1.add(Bytes.toBytes(family), Bytes.toBytes(column),
				Bytes.toBytes(value));
		table.put(put1);
	}

	public void addRecord(Put put) throws Exception {
		try {
			table.put(put);
		} catch (IOException e) {
			logger.error("Error occured while adding a \"Put\" object into table");
			e.printStackTrace();
		}
	}

	public void commit() throws IOException {

		try {
			logger.info("Commiting to HBase");
			table.flushCommits();
		} catch (Exception e) {
			logger.error("Error while flushing commits to table");
			pool.close();
			e.printStackTrace();
		}

		Calendar cal = new GregorianCalendar();
		logger.info("Commited to HStore on: " + cal.getTime().toString());
	}

	@SuppressWarnings("rawtypes")
	public void setColumnValueFilterList(byte[] family,
			Map<byte[], byte[]> filterList) {

		Iterator it = filterList.entrySet().iterator();
		int count = 0;

		while (it.hasNext()) {
			++count;
			Map.Entry pairs = (Map.Entry) it.next();
			byte[] qualifier = (byte[]) pairs.getKey();
			byte[] value = (byte[]) pairs.getValue();
			Filter filter1 =
					new SingleColumnValueFilter(family, qualifier,
							CompareFilter.CompareOp.EQUAL, value);

			filters.add(filter1);
		}
		logger.info(count + " no of rules added to SingleColumnValueFilter.");
	}

	public void setColumnValueFilter(byte[] family, byte[] qualifier,
			String comparator, byte[] value) {

		Filter filter1 = null;

		if (comparator.trim().equals("<"))
			filter1 =
					new SingleColumnValueFilter(family, qualifier,
							CompareFilter.CompareOp.LESS, value);
		else if (comparator.trim().equals("="))
			filter1 =
					new SingleColumnValueFilter(family, qualifier,
							CompareFilter.CompareOp.EQUAL, value);
		else if (comparator.trim().equals(">"))
			filter1 =
					new SingleColumnValueFilter(family, qualifier,
							CompareFilter.CompareOp.GREATER, value);
		else if (comparator.trim().equals("<="))
			filter1 =
					new SingleColumnValueFilter(family, qualifier,
							CompareFilter.CompareOp.GREATER_OR_EQUAL, value);
		else if (comparator.trim().equals(">="))
			filter1 =
					new SingleColumnValueFilter(family, qualifier,
							CompareFilter.CompareOp.LESS_OR_EQUAL, value);
		else
			filter1 =
					new SingleColumnValueFilter(family, qualifier,
							CompareFilter.CompareOp.NO_OP, value);

		filters.add(filter1);

	}

	public void setFilter(String filterType, String comparator,
			byte[] searchParam) {

		Filter tempFilter = null;
		if (filterType.trim().equals("ROW")) { // Row Filter

			if (comparator.trim().equals("<")) {
				tempFilter =
						new RowFilter(CompareFilter.CompareOp.LESS,
								new BinaryComparator(searchParam));
			} else if (comparator.trim().equals(">")) {
				tempFilter =
						new RowFilter(CompareFilter.CompareOp.GREATER,
								new BinaryComparator(searchParam));
			} else if (comparator.trim().equals("=")) {
				tempFilter =
						new RowFilter(CompareFilter.CompareOp.EQUAL,
								new BinaryComparator(searchParam));
			} else if (comparator.trim().equals("<=")) {
				tempFilter =
						new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
								new BinaryComparator(searchParam));
			} else if (comparator.trim().equals(">=")) {
				tempFilter =
						new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
								new BinaryComparator(searchParam));
			} else {
				tempFilter =
						new RowFilter(CompareFilter.CompareOp.NO_OP,
								new BinaryComparator(searchParam));
			}

		} else if (filterType.trim().equals("FAMILY")) { // Family
															// tempFilter

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

		} else if (filterType.trim().equals("QUALIFIER")) { // Qualifier
															// tempFilter

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
				System.out.println("else");
				tempFilter =
						new QualifierFilter(CompareFilter.CompareOp.NO_OP,
								new BinaryComparator(searchParam));
			}
		} else if (filterType.trim().equals("VALUE")) { // Value
														// tempFilter

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

	public ResultScanner getScannerInstance() throws IOException {
		ResultScanner scanner = null;

		scan = new Scan();

		FilterList filterList = new FilterList(filters);
		if (filters != null)
			scan.setFilter(filterList);
		else
			logger.info("No Filters applied.");

		scanner = table.getScanner(scan);

		return scanner;
	}

	public ResultScanner partialScanning(byte[] start, byte[] end) {
		ResultScanner scanner = null;
		try {
			FilterList filterList = new FilterList(filters);
			scan = new Scan();

			if (filters != null)
				scan.setFilter(filterList);
			else
				logger.info("No Filters applied.");

			scan.setStartRow(start);
			scan.setStopRow(end);
			// scan.setCaching(100);
			scanner = table.getScanner(scan);
		} catch (Exception e) {
			logger.error("Error occurred while performing partial scan");
			e.printStackTrace();
		}

		return scanner;
	}
}
