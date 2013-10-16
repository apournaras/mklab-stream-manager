package eu.socialsensor.sfc.streams.store.util;

import java.util.ResourceBundle;

public class ConfigurationUtil {
	private static ResourceBundle rb;

	static {
		rb = ResourceBundle.getBundle("settings");
	}

	public static Integer getNgrams() {
		String ngrams = rb.getString("ngrams");
	    return new Integer(ngrams);
	}
	
	public static String getStopwordFile() {
		return rb.getString("stopwords");
	}
	
	public static String getIndexFile() {
		return rb.getString("indexFile");
	}
}
