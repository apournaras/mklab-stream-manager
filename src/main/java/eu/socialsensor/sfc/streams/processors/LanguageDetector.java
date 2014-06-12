package eu.socialsensor.sfc.streams.processors;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;

import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.sfc.streams.ProcessorConfiguration;

public class LanguageDetector extends Processor {

	public LanguageDetector(ProcessorConfiguration configuration) {
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
			try {
				String title = item.getTitle();
				Detector detector = DetectorFactory.create();
				detector.append(title);
				
				lang = detector.detect();
				item.setLang(lang);
				
			} catch (LangDetectException e) {
				e.printStackTrace();
			}
			
		}
		
	}

}
