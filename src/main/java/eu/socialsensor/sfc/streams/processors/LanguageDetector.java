package eu.socialsensor.sfc.streams.processors;

import twitter4j.internal.logging.Logger;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;

import eu.socialsensor.framework.Configuration;
import eu.socialsensor.framework.common.domain.Item;

public class LanguageDetector extends Processor {

	public LanguageDetector(Configuration configuration) {
		super(configuration);
		String profileDirectory = configuration.getParameter("profileDirectory",
				"profiles.sm");
		try {
			DetectorFactory.loadProfile(profileDirectory);
		} catch (LangDetectException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void process(Item item) {
		String lang = item.getLang();
		if(lang == null) {
			// detect lang
			String text = null;
			String title = item.getTitle();
			String description = item.getDescription();
			
			if(title != null) {
				text = title;
			}
			else if (description != null) {
				text = description;
			}
			else {
				return;
			}
			
			try {
				Detector detector = DetectorFactory.create();
				
				detector.append(text);
				lang = detector.detect();
				item.setLang(lang);
				
			} catch (LangDetectException e) {
				Logger.getLogger(LanguageDetector.class).info("No features in text: " + text);
			}
		}
	}

}
