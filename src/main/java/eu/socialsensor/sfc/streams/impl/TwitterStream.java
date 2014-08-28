package eu.socialsensor.sfc.streams.impl;

import org.apache.log4j.Logger;

import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;
import eu.socialsensor.framework.common.domain.SocialNetworkSource;
import eu.socialsensor.framework.retrievers.socialmedia.TwitterRetriever;
import eu.socialsensor.sfc.streams.Stream;
import eu.socialsensor.sfc.streams.StreamConfiguration;
import eu.socialsensor.sfc.streams.StreamException;

/**
 * Class responsible for setting up the connection to Twitter API
 * for retrieving relevant Twitter content. Handles both the connection
 * to Twitter REST API and Twitter Subscriber. 
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class TwitterStream extends Stream {
	
	public static SocialNetworkSource SOURCE = SocialNetworkSource.Twitter;
	
	private Logger  logger = Logger.getLogger(TwitterStream.class);

	@Override
	public synchronized void open(StreamConfiguration config) throws StreamException {

		logger.info("#Twitter : Open stream");
		
		if (config == null) {
			logger.error("#Twitter : Config file is null.");
			return;
		}
		
		String oAuthConsumerKey 		= 	config.getParameter(KEY);
		String oAuthConsumerSecret 		= 	config.getParameter(SECRET);
		String oAuthAccessToken 		= 	config.getParameter(ACCESS_TOKEN);
		String oAuthAccessTokenSecret 	= 	config.getParameter(ACCESS_TOKEN_SECRET);
		
		if (oAuthConsumerKey == null || oAuthConsumerSecret == null ||
				oAuthAccessToken == null || oAuthAccessTokenSecret == null) {
			logger.error("#Twitter : Stream requires authentication");
			throw new StreamException("Stream requires authentication");
		}
		
		logger.info("Twitter Credentials: \n" + 
				"\t\t\toAuthConsumerKey:  " + oAuthConsumerKey  + "\n" +
				"\t\t\toAuthConsumerSecret:  " + oAuthConsumerSecret  + "\n" +
				"\t\t\toAuthAccessToken:  " + oAuthAccessToken + "\n" +
				"\t\t\toAuthAccessTokenSecret:  " + oAuthAccessTokenSecret);
		
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setJSONStoreEnabled(false)
			.setOAuthConsumerKey(oAuthConsumerKey)
			.setOAuthConsumerSecret(oAuthConsumerSecret)
			.setOAuthAccessToken(oAuthAccessToken)
			.setOAuthAccessTokenSecret(oAuthAccessTokenSecret);
		Configuration conf = cb.build();
		

		logger.info("Initialize Twitter Retriever for REST api");

		String maxRequests = config.getParameter(MAX_REQUESTS);
		String maxResults = config.getParameter(MAX_RESULTS);
		String maxRunningTime = config.getParameter(MAX_RUNNING_TIME);
			
		retriever = new TwitterRetriever(conf, Integer.parseInt(maxRequests), Integer.parseInt(maxResults), Long.parseLong(maxRunningTime));	
	}
	
	@Override
	public String getName() {
		return "Twitter";
	}
}

