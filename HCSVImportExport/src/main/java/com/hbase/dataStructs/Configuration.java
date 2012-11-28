package com.hbase.dataStructs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class Configuration {

	private String zkClientPort;
	private String zkQuorum;
	private String znodeParent;
	private String tableName;
	private String columnFamily;
	private String columnFields;
	private String columnTypes;

	public boolean loadConfiguration(String filePath) {
		boolean isError = false;
		Properties props = new Properties();
		if (filePath != null) {
			try {
				FileInputStream fin = new FileInputStream(filePath);
				props.load(fin);
			} catch (FileNotFoundException e) {
				isError = true;
				e.printStackTrace();
			} catch (IOException e) {
				isError = true;
				e.printStackTrace();
			}
		}

		if (isError) {
			setTableName(props.getProperty("table"));
			setColumnFamily(props.getProperty("family"));
			setColumnFields(props.getProperty("fields"));
			setColumnTypes(props.getProperty("fieldTypes"));

			setZkClientPort(props.getProperty("zookeeper.clientPort"));
			setZkQuorum(props.getProperty("zookeeper.quorum"));
			setZnodeParent(props.getProperty("zookeeper.znodeParent"));

		}

		return isError;
	}

	/**
	 * @return the zkClientPort
	 */
	public String getZkClientPort() {
		return zkClientPort;
	}

	/**
	 * @param zkClientPort
	 *            the zkClientPort to set
	 */
	public void setZkClientPort(String zkClientPort) {
		this.zkClientPort = zkClientPort;
	}

	/**
	 * @return the zkQuorum
	 */
	public String getZkQuorum() {
		return zkQuorum;
	}

	/**
	 * @param zkQuorum
	 *            the zkQuorum to set
	 */
	public void setZkQuorum(String zkQuorum) {
		this.zkQuorum = zkQuorum;
	}

	/**
	 * @return the znodeParent
	 */
	public String getZnodeParent() {
		return znodeParent;
	}

	/**
	 * @param znodeParent
	 *            the znodeParent to set
	 */
	public void setZnodeParent(String znodeParent) {
		this.znodeParent = znodeParent;
	}

	/**
	 * @return the tableName
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * @param tableName
	 *            the tableName to set
	 */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	/**
	 * @return the columnFamily
	 */
	public String getColumnFamily() {
		return columnFamily;
	}

	/**
	 * @param columnFamily
	 *            the columnFamily to set
	 */
	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	/**
	 * @return the columnFields
	 */
	public String getColumnFields() {
		return columnFields;
	}

	/**
	 * @param columnFields
	 *            the columnFields to set
	 */
	public void setColumnFields(String columnFields) {
		this.columnFields = columnFields;
	}

	/**
	 * @return the columnTypes
	 */
	public String getColumnTypes() {
		return columnTypes;
	}

	/**
	 * @param columnTypes
	 *            the columnTypes to set
	 */
	public void setColumnTypes(String columnTypes) {
		this.columnTypes = columnTypes;
	}
}
