package com.conagg.hbase.ds;

import java.util.List;

import org.hsqldb.Column;

import com.conagg.hbase.utils.ByteArrayWrapper;

@SuppressWarnings("javadoc")
public class Row {

	private ByteArrayWrapper rowKeyArray;
	private List<Column> columns;
}
