package eu.socialsensor.sfc.streams.impl;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.SocialNetworkSource;
import eu.socialsensor.framework.retrievers.socialmedia.FlickrRetriever;
import eu.socialsensor.sfc.streams.Stream;
import eu.socialsensor.sfc.streams.StreamConfiguration;
import eu.socialsensor.sfc.streams.StreamException;

/**
 * Class responsible for setting up the connection to Flickr API
 * for retrieving relevant Flickr content.
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class FlickrStream extends Stream {

	private Logger logger = Logger.getLogger(FlickrStream.class);
	
	public static final SocialNetworkSource SOURCE = SocialNetworkSource.Flickr;
	
	private String key;
	private String secret;

	
	@Override
	public void open(StreamConfiguration config) throws StreamException {
		logger.info("#Flickr : Open stream");
		
		if (config == null) {
			logger.error("#Flickr : Config file is null.");
			return;
		}
		
		key = config.getParameter(KEY);
		secret = config.getParameter(SECRET);
		
		String maxResults = config.getParameter(MAX_RESULTS);
		String maxRequests = config.getParameter(MAX_REQUESTS);
		
		String maxRunningTime = config.getParameter(MAX_RUNNING_TIME);
		
		if (key == null || secret==null) {
			logger.error("#Flickr : Stream requires authentication.");
			throw new StreamException("Stream requires authentication.");
		}
		
		retriever = new FlickrRetriever(key, secret,Integer.parseInt(maxResults),Integer.parseInt(maxRequests),Long.parseLong(maxRunningTime));
		
	}
	
	@Override
	public String getName() {
		return "Flickr";
	}
	
}
