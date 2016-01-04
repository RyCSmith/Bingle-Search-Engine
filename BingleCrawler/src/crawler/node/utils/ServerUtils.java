package crawler.node.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class ServerUtils {
	private final static SimpleDateFormat stdFmt = new SimpleDateFormat(
			"EEE, d MMM yyyy HH:mm:ss");
	// FMT: Friday, 31-Dec-99 23:59:59 GMT
	private final static SimpleDateFormat fmt2 = new SimpleDateFormat(
			"E, d-MMM-yy HH:mm:ss");
	// FMT: Fri Dec 31 23:59:59 1999
	private final static SimpleDateFormat fmt3 = new SimpleDateFormat(
			"EEE MMM d HH:mm:ss yyyy");
	private final static TimeZone tz = TimeZone.getTimeZone("GMT");

	private final static SimpleDateFormat hw2Fmt = new SimpleDateFormat(
			"yyyy-MM-dd");
	private final static SimpleDateFormat hw2Fmt1 = new SimpleDateFormat(
			"HH:mm:ss");
	
	public static String getDate() {
		return getDate(new Date());
	}

	public static String getDate(long l) {
		return getDate(new Date(l));
	}
	
	public static String getHw2Date(long l) {
		return hw2Fmt.format(new Date(l)) + "T" + hw2Fmt1.format(new Date(l));
	}

	public static String getDate(Date d) {
		stdFmt.setTimeZone(tz);
		return stdFmt.format(d);
	}

	public static Date dateFromString(String dateS) {
		Date ims = null;
		stdFmt.setTimeZone(tz);
		try {
			ims = stdFmt.parse(dateS);
		} catch (ParseException e) {
			try {
				fmt2.setTimeZone(tz);
				ims = fmt2.parse(dateS);
			} catch (ParseException _) {
				try {
					fmt3.setTimeZone(tz);
					ims = fmt3.parse(dateS);
				} catch (ParseException e2) {
				}
			}
		}
		return ims;
	}
}
