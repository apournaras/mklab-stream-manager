package eu.socialsensor.sfc.streams.filters;

import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.sfc.streams.FilterConfiguration;

public class LengthItemFilter extends ItemFilter {

	private int minTextLenth = 10;

	public LengthItemFilter(FilterConfiguration configuration) {
		super(configuration);
		String lenStr =configuration.getParameter("length", "10");
		this.minTextLenth  = Integer.parseInt(lenStr);
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
