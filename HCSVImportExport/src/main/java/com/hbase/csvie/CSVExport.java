package com.hbase.csvie;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import com.hbase.dataStructs.Configuration;
import com.hbase.dataStructs.ConnectionConf;
import com.hbase.dataStructs.Constants;
import com.hbase.dataStructs.Row;
import com.hbase.dataStructs.TableConf;
import com.hbase.dataStructs.Utils;
import com.hbase.reader.HInterface;

public class CSVExport {

	/**
	 * Takes in two parameters 1. date (yyyy-MM-dd) / hour 2. filename
	 * (with extension)
	 * 
	 * @param args
	 */
	public static void main(String args[]) {

		if (args[0] == null || args[0].isEmpty() || args[0].equals("-help")
				|| args[1] == null || args[1].isEmpty() || args[2] == null
				|| args[2].isEmpty() || args.length > 3) {

			System.out.println("Invalid input");
			System.out.println("Use: ");
			System.out.println("=====");
			System.out.println("1. date(yyyy-MM-dd) hour(24 hr) filename.csv");
			System.out
					.println("-Export data for a particular hour of a particular day");
			System.out.println();
			System.out.println("2. date(yyyy-MM-dd) nil filename.csv");
			System.out.println("-Export data for a particular day");

			System.exit(1);
		}

		String date = args[0];
		String hour = args[1];
		String filePath = args[2];

		if (!Utils.isDateValid(date, Constants.dateFormat, false)
				|| !hour.equals("nil") && !Utils.isHourValid(hour)
				|| !Utils.isPathValid(filePath)) {
			System.err.println("Invalid date/hour provided");
			System.out.println("Quitting....");
			System.exit(1);
		}

		init(date, hour, filePath);
	}

	private static void init(String date, String hour, String filePath) {

		Configuration config = new Configuration();
		config.loadConfiguration(filePath);

		TableConf tconf = new TableConf();
		tconf.setConfiguration(config);

		ConnectionConf cconf = new ConnectionConf();
		cconf.setConfiguration(config);

		HInterface hInterface = new HInterface();

		try {
			hInterface.init(cconf, tconf);
		} catch (IOException e) {
			e.printStackTrace();
		}

		exportToCSV(hInterface, tconf);

	}

	private static void exportToCSV(HInterface hInterface, TableConf tconf) {

		ResultScanner scanner = null;
		String[] field = tconf.getFields().split(",");
		String[] types = tconf.getFieldTypes().split(",");
		String[] value = new String[field.length];

		Row row = new Row();

		try {
			scanner = hInterface.getScannerInstance();
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (Result res : scanner) {
			for (int i = 0; i < field.length; i++) {
				// get data as per fields and set them to row
				// finally insert the row into CSV using an API
			}
		}
	}
}
