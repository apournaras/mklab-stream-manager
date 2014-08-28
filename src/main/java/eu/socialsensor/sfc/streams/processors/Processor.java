package eu.socialsensor.sfc.streams.processors;

import eu.socialsensor.framework.Configuration;
import eu.socialsensor.framework.common.domain.Item;

public abstract class Processor {

	@SuppressWarnings("unused")
	private Configuration configuration;

	public Processor(Configuration configuration) {
		this.configuration = configuration;
	}
	
	public abstract  void process(Item item);
	
}
