package eu.socialsensor.sfc.streams.filters;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.Configuration;
import eu.socialsensor.framework.common.domain.Item;

public class LengthItemFilter extends ItemFilter {

	private int minTextLenth = 10;

	public LengthItemFilter(Configuration configuration) {
		super(configuration);
		String lenStr =configuration.getParameter("length", "10");
		this.minTextLenth  = Integer.parseInt(lenStr);
		
		Logger.getLogger(LengthItemFilter.class).info("Initialized. Min Text Lenth: " + minTextLenth);
	}
	
	@Override
	public boolean accept(Item item) {
		if(item == null) {
			incrementDiscarded();
			return false;
		}
		
		String title = item.getTitle();
		if(title == null) {
			incrementDiscarded();
			return false;
		}
		
		if(title.length() < minTextLenth) {
			incrementDiscarded();
			return false;
		}
		
		incrementAccepted();
		return true;
	}

	@Override
	public String name() {
		return "LengthItemFilter";
	}
}
