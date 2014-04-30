package eu.socialsensor.sfc.streams.filters;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.sfc.streams.FilterConfiguration;

/**
 * 
 * @author Manos Schinas - manosetro@iti.gr
 *
 * This filter discard items that have many hashtags as possible spam.
 * 	
 */
public class TagsItemFilter extends ItemFilter {

	private int maxTags = 4;

	public TagsItemFilter(FilterConfiguration configuration) {
		super(configuration);
		String lenStr =configuration.getParameter("maxTags", "4");
		this.maxTags  = Integer.parseInt(lenStr);
		
		Logger.getLogger(TagsItemFilter.class).info("Initialized. Max Number of Tags: " + maxTags);
	}
	
	@Override
	public boolean accept(Item item) {
		if(item == null) {
			return false;
		}
		
		String[] tags = item.getTags();
		if(tags == null) {
			return true;
		}
		
		if(tags.length > maxTags) {
			return false;
		}
		
		return true;
	}

}
