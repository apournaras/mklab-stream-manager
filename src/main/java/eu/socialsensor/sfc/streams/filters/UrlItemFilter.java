package eu.socialsensor.sfc.streams.filters;

import java.net.URL;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.sfc.streams.FilterConfiguration;

/**
 * 
 * @author Manos Schinas - manosetro@iti.gr
 *
 * This filter discard items that have many embedded URLs as possible spam.
 * 	
 */
public class UrlItemFilter extends ItemFilter {

	private int maxUrls = 4;

	public UrlItemFilter(FilterConfiguration configuration) {
		super(configuration);
		String lenStr =configuration.getParameter("maxUrls", "4");
		this.maxUrls  = Integer.parseInt(lenStr);
		
		Logger.getLogger(UrlItemFilter.class).info("Initialized. Max Number of URLs: " + maxUrls);
	}
	
	@Override
	public boolean accept(Item item) {
		if(item == null) {
			return false;
		}
		
		URL[] urls = item.getLinks();
		if(urls == null) {
			return true;
		}
		
		if(urls.length > maxUrls) {
			System.out.println("Item " + item.getId() + " is possible spam due to URLs number (" + urls.length + ")");
			return false;
		}
		
		return true;
	}

}
