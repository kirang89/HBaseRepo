package com.conagg.hbase.ds;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Data Structure to convert a date of format yyyy-MM-dd to
 * bytes
 * 
 * @author komlimobile
 * 
 */
public class DateCell {
	int start;
	byte[] key;

	public DateCell(int start, byte[] key) {
		this.start = start;
		this.key = key;
	}

	public byte[] write(String val) throws ParseException {
		if (!val.isEmpty()) {
			DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			Date date = formatter.parse(val);
			long dateInLong = date.getTime();
			Bytes.putLong(key, start, dateInLong);
		} else {

			Bytes.putLong(key, start, Long.valueOf(0));
		}
		return key;

	}

	public long read(byte[] key) {
		return Bytes.toLong(key, start);
	}
}
