package eu.socialsensor.sfc.streams.filters;

import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.sfc.streams.FilterConfiguration;

public abstract class ItemFilter {
	
	@SuppressWarnings("unused")
	private FilterConfiguration configuration;

	public ItemFilter(FilterConfiguration configuration) {
		this.configuration = configuration;
	}
	
	public abstract boolean accept(Item item);
}
