package gr.iti.mklab.sfc.filters;

import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.config.Configuration;

import org.apache.logging.log4j.LogManager;

/**
 * 
 * @author Manos Schinas - manosetro@iti.gr
 *
 * This filter discard items that have many hashtags as possible spam.
 * 	
 */
public class TagsItemFilter extends ItemFilter {

	private int maxTags = 4;
	
	public TagsItemFilter(Configuration configuration) {
		super(configuration);
		String lenStr =configuration.getParameter("maxTags", "4");
		this.maxTags  = Integer.parseInt(lenStr);
		
		LogManager.getLogger(TagsItemFilter.class).info("Initialized. Max Number of Tags: " + maxTags);
	}
	
	@Override
	public synchronized boolean accept(Item item) {
		if(item == null) {
			incrementDiscarded();
			return false;
		}
		
		String[] tags = item.getTags();
		if(tags == null) {
			incrementAccepted();
			return true;
		}
		
		if(tags.length >= maxTags) {
			incrementDiscarded();
			return false;
		}
		
		incrementAccepted();
		return true;
	}

	@Override
	public String name() {
		return "TagsItemFilter";
	}
	
}
