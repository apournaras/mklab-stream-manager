package eu.socialsensor.sfc.streams.filters;

import java.net.URL;

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

	private int maxUrl = 4;

	public UrlItemFilter(FilterConfiguration configuration) {
		super(configuration);
		String lenStr =configuration.getParameter("maxUrl", "4");
		this.maxUrl  = Integer.parseInt(lenStr);
		
		System.out.println("Max Number of URLs: " + maxUrl);
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
		
		if(urls.length > maxUrl) {
			return false;
		}
		
		return true;
	}

}
