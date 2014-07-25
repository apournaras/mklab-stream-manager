package eu.socialsensor.sfc.streams.filters;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.sfc.streams.FilterConfiguration;

public class LanguageItemFilter extends ItemFilter {

	private Set<String> languages = new HashSet<String>();

	public LanguageItemFilter(FilterConfiguration configuration) {
		super(configuration);
		String langsStr = configuration.getParameter("lang", "en");
		
		String[] langs = langsStr.split(",");
		for(String lang : langs) {
			if(lang != null)
				languages.add(lang.trim());
		}
		
		Logger.getLogger(LengthItemFilter.class).info("Supported languages: " + langsStr);
	}
	
	@Override
	public boolean accept(Item item) {
		
		String lang = item.getLang();
		if(lang == null) {
			incrementAccepted();
			return true;
		}
		
		if(!languages.contains(lang)) {
			incrementDiscarded();
			return false;
		}
		
		incrementAccepted();
		return true;
	}

	@Override
	public String name() {
		return "LanguageItemFilter";
	}

}
