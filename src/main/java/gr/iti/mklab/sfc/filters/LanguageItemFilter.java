package gr.iti.mklab.sfc.filters;

import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.config.Configuration;

import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;


public class LanguageItemFilter extends ItemFilter {

	private Set<String> languages = new HashSet<String>();

	public LanguageItemFilter(Configuration configuration) {
		super(configuration);
		String langsStr = configuration.getParameter("lang", "en");
		
		String[] langs = langsStr.split(",");
		for(String lang : langs) {
			if(lang != null) {
				languages.add(lang.trim());
			}
		}
		LogManager.getLogger(LanguageItemFilter.class).info("Supported languages: " + langsStr);
	}
	
	@Override
	public boolean accept(Item item) {
		
		String lang = item.getLanguage();
		if(lang == null) {
			incrementDiscarded();
			return false;
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
