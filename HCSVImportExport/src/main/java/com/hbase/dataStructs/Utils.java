package com.hbase.dataStructs;

import java.io.File;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {

	public static boolean isDateValid(String dateStr, String format,
			boolean lenient) {
		@SuppressWarnings("unused")
		Date date = null;
		boolean error = false;

		// test date string matches format structure using regex
		// - weed out illegal characters and enforce 4-digit year
		// - create the regex based on the local format string

		String reFormat =
				Pattern.compile("d+|M+")
						.matcher(Matcher.quoteReplacement(format))
						.replaceAll("\\\\d{1,2}");
		reFormat =
				Pattern.compile("y+").matcher(reFormat).replaceAll("\\\\d{4}");
		if (Pattern.compile(reFormat).matcher(dateStr).matches()) {
			// date string matches format structure,
			// - now test it can be converted to a valid date
			SimpleDateFormat sdf =
					(SimpleDateFormat) DateFormat.getDateInstance();
			sdf.applyPattern(format);
			sdf.setLenient(lenient);
			try {
				date = sdf.parse(dateStr);
			} catch (ParseException e) {
				error = true;
			}
		}
		return error;
	}

	public static boolean isHourValid(String input) {
		boolean error = false;
		int hour = 0;

		try {
			hour = Integer.parseInt(input);
		} catch (Exception e) {
			error = true;
		}

		if (hour < 0 || hour > 24)
			error = true;

		return error;
	}

	public static boolean isPathValid(String filePath) {
		boolean error = false;

		if (!new File(filePath).exists())
			error = true;

		return error;
	}
}
