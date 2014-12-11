package gr.iti.mklab.sfc.streams.processors;

import gr.iti.mklab.framework.common.domain.Configuration;
import gr.iti.mklab.framework.common.domain.Item;

public abstract class Processor {

	@SuppressWarnings("unused")
	private Configuration configuration;

	public Processor(Configuration configuration) {
		this.configuration = configuration;
	}
	
	public abstract  void process(Item item);
	
}
