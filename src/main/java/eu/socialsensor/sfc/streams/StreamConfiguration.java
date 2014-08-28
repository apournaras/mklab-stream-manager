package eu.socialsensor.sfc.streams;

import eu.socialsensor.framework.Configuration;
import eu.socialsensor.sfc.streams.management.StorageHandler;

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
