package eu.socialsensor.sfc.streams.impl;


import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.SocialNetworkSource;
import eu.socialsensor.framework.retrievers.socialmedia.TumblrRetriever;
import eu.socialsensor.sfc.streams.Stream;
import eu.socialsensor.sfc.streams.StreamConfiguration;
import eu.socialsensor.sfc.streams.StreamException;


/**
 * Class responsible for setting up the connection to Tumblr API
 * for retrieving relevant Tumblr content.
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class TumblrStream extends Stream {
	
	public static final SocialNetworkSource SOURCE = SocialNetworkSource.Tumblr;
	
	private String consumerKey;
	private String consumerSecret;
	
	private Logger logger = Logger.getLogger(TumblrStream.class);

	
	@Override
	public void open(StreamConfiguration config) throws StreamException {
		logger.info("#Tumblr : Open stream");
		
		if (config == null) {
			logger.error("#Tumblr : Config file is null.");
			return;
		}
		
		consumerKey = config.getParameter(KEY);
		consumerSecret = config.getParameter(SECRET);
		
		String maxResults = config.getParameter(MAX_RESULTS);
		String maxRequests = config.getParameter(MAX_REQUESTS);
		String maxRunningTime = config.getParameter(MAX_RUNNING_TIME);
		
		if (consumerKey == null || consumerSecret==null) {
			logger.error("#Tumblr : Stream requires authentication.");
			throw new StreamException("Stream requires authentication.");
		}
		
		retriever = new TumblrRetriever(consumerKey,consumerSecret,Integer.parseInt(maxResults),Integer.parseInt(maxRequests),Long.parseLong(maxRunningTime));
		
	}

	@Override
	public String getName() {
		return "Tumblr";
	}
	
}
