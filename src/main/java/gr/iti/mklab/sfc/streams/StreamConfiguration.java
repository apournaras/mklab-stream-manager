package gr.iti.mklab.sfc.streams;

import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.sfc.streams.management.StorageHandler;

/**
 * The configuration of the streams that are going to be used
 * for the retrieval process

 */
public class StreamConfiguration extends Configuration {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -8712318419026334808L;
	
	private StorageHandler handler;
	
	public StorageHandler getHandler() {
		return handler;
	}
	
}
