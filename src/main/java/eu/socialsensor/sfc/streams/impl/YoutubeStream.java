package eu.socialsensor.sfc.streams.impl;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.SocialNetworkSource;
import eu.socialsensor.framework.retrievers.socialmedia.YoutubeRetriever;
import eu.socialsensor.sfc.streams.Stream;
import eu.socialsensor.sfc.streams.StreamConfiguration;
import eu.socialsensor.sfc.streams.StreamException;


/**
 * Class responsible for setting up the connection to Google API
 * for retrieving relevant YouTube content.
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class YoutubeStream extends Stream {

	public static SocialNetworkSource SOURCE = SocialNetworkSource.Youtube;
	
	private Logger logger = Logger.getLogger(YoutubeStream.class);
	
	private String clientId;
	private String developerKey;

	
	@Override
	public void close() throws StreamException {
		if(monitor != null)
			monitor.stopMonitor();
		logger.info("#YouTube : Close stream");
	}
	
	@Override
	public void open(StreamConfiguration config) throws StreamException {
		logger.info("#YouTube : Open stream");
		
		if (config == null) {
			logger.error("#YouTube : Config file is null.");
			return;
		}
		
		this.clientId = config.getParameter(CLIENT_ID);
		this.developerKey = config.getParameter(KEY);
		String maxResults = config.getParameter(MAX_RESULTS);
		String maxRequests = config.getParameter(MAX_REQUESTS);
		String maxRunningTime = config.getParameter(MAX_RUNNING_TIME);
		
		if (clientId == null || developerKey == null) {
			logger.error("#YouTube : Stream requires authentication.");
			throw new StreamException("Stream requires authentication");
		}

		retriever = new YoutubeRetriever(clientId, developerKey,Integer.parseInt(maxResults),Integer.parseInt(maxRequests),Long.parseLong(maxRunningTime));

	}
	
	@Override
	public String getName() {
		return "YouTube";
	}
	
}
