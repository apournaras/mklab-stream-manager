package eu.socialsensor.sfc.streams.impl;


import org.apache.log4j.Logger;

import com.restfb.DefaultFacebookClient;
import com.restfb.FacebookClient;

import eu.socialsensor.framework.common.domain.SocialNetworkSource;
import eu.socialsensor.framework.retrievers.socialmedia.FacebookRetriever;
import eu.socialsensor.sfc.streams.Stream;
import eu.socialsensor.sfc.streams.StreamConfiguration;
import eu.socialsensor.sfc.streams.StreamException;

/**
 * Class responsible for setting up the connection to Facebook API
 * for retrieving relevant Facebook content.
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class FacebookStream extends Stream {
	
	public static SocialNetworkSource SOURCE = SocialNetworkSource.Facebook;
	
	public int maxFBRequests = 600;
	public long minInterval = 600000;
	
	private Logger  logger = Logger.getLogger(FacebookStream.class);
	
	private FacebookClient facebookClient;
	
	
	@Override
	public synchronized void open(StreamConfiguration config) throws StreamException {
		logger.info("#Facebook : Open stream");
		
		if (config == null) {
			logger.error("#Facebook : Config file is null.");
			return;
		}
		
		
		String access_token = config.getParameter(ACCESS_TOKEN);
		String app_id = config.getParameter(APP_ID);
		String app_secret = config.getParameter(APP_SECRET);
		
		int maxResults = Integer.parseInt(config.getParameter(MAX_RESULTS));
		int maxRequests = Integer.parseInt(config.getParameter(MAX_REQUESTS));
		
		if(maxRequests > maxFBRequests)
			maxRequests = maxFBRequests;   
		
		long maxRunningTime = Long.parseLong(config.getParameter(MAX_RUNNING_TIME));
		
		if (access_token == null && app_id == null && app_secret == null) {
			logger.error("#Facebook : Stream requires authentication.");
			throw new StreamException("Stream requires authentication.");
		}
		
		
		if(access_token == null)
			access_token = app_id+"|"+app_secret;
		
		facebookClient = new DefaultFacebookClient(access_token);
		retriever = new FacebookRetriever(facebookClient, maxRequests, minInterval, maxResults, maxRunningTime);	

	}

	@Override
	public String getName() {
		return "Facebook";
	}
	
}
