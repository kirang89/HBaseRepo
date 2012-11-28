package com.hbase.dataStructs;

public class Row {

	private String[] cHeader;
	private String[] cValue;

	/**
	 * @return the field
	 */
	public String[] getColumnHeaders() {
		return cHeader;
	}

	/**
	 * @param field
	 *            the field to set
	 */
	public void setColumnHeaders(String[] cHeader) {
		this.cHeader = cHeader;
	}

	/**
	 * @return the cValue
	 */
	public String[] getColumnValues() {
		return cValue;
	}

	/**
	 * @param cValue
	 *            the cValue to set
	 */
	public void setColumnValues(String[] cValue) {
		this.cValue = cValue;
	}

}
