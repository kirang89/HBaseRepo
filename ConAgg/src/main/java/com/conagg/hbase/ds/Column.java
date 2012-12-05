package com.conagg.hbase.ds;

/**
 * Column class that represents a column of a table
 * 
 * @author <a HREF="mailto:kiran.daredevil@gmail.com">Kiran
 *         Gangadharan</a>
 * 
 */
@SuppressWarnings("javadoc")
public class Column {

	public String fieldName;
	public Object fieldValue;
	public char fieldType;
	public long freqCounter = 0;
	public boolean computeSum;
	public boolean computeAverage;
}
