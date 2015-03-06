package gr.iti.mklab.sfc.streams.impl;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.common.domain.Source;
import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.retrievers.impl.FacebookRetriever;
import gr.iti.mklab.sfc.streams.Stream;
import gr.iti.mklab.sfc.streams.StreamException;
import gr.iti.mklab.sfc.streams.monitors.RateLimitsMonitor;

/**
 * Class responsible for setting up the connection to Facebook API
 * for retrieving relevant Facebook content.
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 */
public class FacebookStream extends Stream {
	
	public static Source SOURCE = Source.Facebook;
	
	public int maxFBRequests = 600;
	public long minInterval = 600000;
	
	private Logger  logger = Logger.getLogger(FacebookStream.class);	
	
	@Override
	public synchronized void open(Configuration config) throws StreamException {
		logger.info("#Facebook : Open stream");
		
		if (config == null) {
			logger.error("#Facebook : Config file is null.");
			return;
		}
		
		
		String accessToken = config.getParameter(ACCESS_TOKEN);
		String key = config.getParameter(KEY);
		String secret = config.getParameter(SECRET);
		
		int maxRequests = Integer.parseInt(config.getParameter(MAX_REQUESTS));
		
		if(maxRequests > maxFBRequests)
			maxRequests = maxFBRequests;   
		
		if (accessToken == null && key == null && secret == null) {
			logger.error("#Facebook : Stream requires authentication.");
			throw new StreamException("Stream requires authentication.");
		}
		
		
		if(accessToken == null) {
			accessToken = key + "|" + secret;
		}
		
		Credentials credentials = new Credentials();
		credentials.setAccessToken(accessToken);
		
		rateLimitsMonitor = new RateLimitsMonitor(maxRequests, minInterval);
		
		retriever = new FacebookRetriever(credentials);	
	}

	@Override
	public String getName() {
		return "Facebook";
	}
	
}
