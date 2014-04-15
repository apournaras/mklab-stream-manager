package eu.socialsensor.sfc.streams.filters;

import eu.socialsensor.framework.common.domain.Item;

public interface ItemFilter {
	public boolean accept(Item item);
}
