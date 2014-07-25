package eu.socialsensor.sfc.streams.filters;

import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.sfc.streams.FilterConfiguration;

public abstract class ItemFilter {
	
	@SuppressWarnings("unused")
	private FilterConfiguration configuration;

	private int discarded = 0;
	private int accepted = 0;

	public ItemFilter(FilterConfiguration configuration) {
		this.configuration = configuration;
	}
	
	public abstract boolean accept(Item item);
	
	public abstract String name();
	
	public String status() {
		return discarded + " items discarded, " + accepted + " items accepted.";
	}
	
	public void incrementAccepted() {
		synchronized (this) {
	    	accepted++;
	    }
	}
	
	public void incrementDiscarded() {
		synchronized (this) {
			discarded++;
	    }
	}
	
}
