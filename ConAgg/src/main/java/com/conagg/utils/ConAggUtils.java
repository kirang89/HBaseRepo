package com.conagg.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.conagg.constants.Constants;

/**
 * @author <a HREF="mailto:kiran.daredevil@gmail.com">Kiran
 *         Gangadharan</a>
 * 
 */
public class ConAggUtils {

	/**
	 * Check whether given date is valid
	 * 
	 * @param dateString
	 * @param format
	 * @return boolean
	 */
	@SuppressWarnings("unused")
	public static boolean isValidDate(String dateString, String format) {
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		sdf.setLenient(false);

		try {
			Date date = sdf.parse(dateString);
		} catch (ParseException e) {
			return false;
		}

		return true;
	}

	/**
	 * Get date in long format(milliseconds)
	 * 
	 * @param dateStr
	 * @return date in milliseconds
	 */
	public static long getLongDate(String dateStr) {
		Date date1;
		try {
			date1 = Constants.outputDateFormat.parse(dateStr);
		} catch (ParseException e) {
			e.printStackTrace();
			return 0;
		}
		return date1.getTime();
	}

	/**
	 * Check whether given hour is valid
	 * 
	 * @param hourString
	 * @return boolean
	 */
	public static boolean isValidHour(String hourString) {
		try {
			int hour = Integer.parseInt(hourString);
			if (hour > -1 && hour < 60)
				return true;
			else
				return false;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * Create a logger(log4j) instance
	 * 
	 * @param loggerName
	 *            logger instance name
	 * @param logFile
	 *            path to log file
	 * @param level
	 *            logger level
	 * @return <code>Logger</code>
	 */
	public static Logger createLogger(String loggerName, String logFile,
			Level level) {
		Logger logger = Logger.getLogger(loggerName);
		FileAppender fa = new FileAppender();
		fa.setName(loggerName);
		fa.setFile(logFile);
		fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
		fa.setThreshold(level);
		fa.setAppend(false);
		fa.activateOptions();
		logger.addAppender(fa);

		return logger;
	}
}
