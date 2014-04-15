package eu.socialsensor.sfc.streams.filters;

import eu.socialsensor.framework.common.domain.Item;

public class LengthItemFilter implements ItemFilter {

	private int minTextLenth = 10;

	public LengthItemFilter(int minTextLenth) {
		this.minTextLenth  = minTextLenth;
	}
	
	@Override
	public boolean accept(Item item) {
		if(item == null)
			return false;
		
		String title = item.getTitle();
		if(title == null)
			return false;
		
		if(title.length() < minTextLenth)
			return false;
		
		return true;
	}

}
