package gr.iti.mklab.sfc.streams.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.common.domain.Source;
import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.retrievers.impl.YoutubeRetriever;
import gr.iti.mklab.sfc.streams.Stream;
import gr.iti.mklab.sfc.streams.StreamException;
import gr.iti.mklab.sfc.streams.monitors.RateLimitsMonitor;

/**
 * Class responsible for setting up the connection to Google API
 * for retrieving relevant YouTube content.
 * 
 * @author manosetro - manosetro@iti.gr
 */
public class YoutubeStream extends Stream {

	public static Source SOURCE = Source.Youtube;
	
	private Logger logger = LogManager.getLogger(YoutubeStream.class);
	
	private String developerKey;
	
	@Override
	public void close() throws StreamException {
		logger.info("#YouTube : Close stream");
	}
	
	@Override
	public void open(Configuration config) throws StreamException {
		logger.info("#YouTube : Open stream");
		
		if (config == null) {
			logger.error("#YouTube : Config file is null.");
			return;
		}
		
		this.developerKey = config.getParameter(KEY);
		
		if (developerKey == null) {
			logger.error("#YouTube : Stream requires authentication.");
			throw new StreamException("Stream requires authentication");
		}

		Credentials credentials = new Credentials();
		credentials.setKey(developerKey);
		
		maxRequests = Integer.parseInt(config.getParameter(MAX_REQUESTS));
		timeWindow = Long.parseLong(config.getParameter(TIME_WINDOW));
		
		rateLimitsMonitor = new RateLimitsMonitor(maxRequests, timeWindow);
		
		retriever = new YoutubeRetriever(credentials);

	}
	
	@Override
	public String getName() {
		return SOURCE.name();
	}
	
}
