package eu.socialsensor.sfc.streams.processors;

import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.sfc.streams.ProcessorConfiguration;

public abstract class Processor {

	@SuppressWarnings("unused")
	private ProcessorConfiguration configuration;

	public Processor(ProcessorConfiguration configuration) {
		this.configuration = configuration;
	}
	
	public abstract  void process(Item item);
	
}
