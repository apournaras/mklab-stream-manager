package eu.socialsensor.sfc.streams.filters;

import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.sfc.streams.FilterConfiguration;

public class LengthItemFilter extends ItemFilter {

	private int minTextLenth = 10;

	public LengthItemFilter(FilterConfiguration configuration) {
		super(configuration);
		String lenStr =configuration.getParameter("length", "10");
		this.minTextLenth  = Integer.parseInt(lenStr);
		
		System.out.println("Min Text Lenth: " + minTextLenth);
	}
	
	@Override
	public boolean accept(Item item) {
		if(item == null) {
			//System.out.println("Null item");
			return false;
		}
		
		String title = item.getTitle();
		if(title == null) {
			//System.out.println(item.getId() + " has null text");
			return false;
		}
		
		if(title.length() < minTextLenth) {
			//System.out.println(item.getTitle() + " filtered out due to text length (" + title.length() + ")");
			return false;
		}
		
		return true;
	}

}
