package eu.socialsensor.sfc.streams.impl;

import eu.socialsensor.framework.common.domain.NewsFeedSource;
import eu.socialsensor.framework.retrievers.newsfeed.RSSRetriever;
import eu.socialsensor.sfc.streams.Stream;
import eu.socialsensor.sfc.streams.StreamConfiguration;

/**
 * Class responsible for setting up the connection for retrieving RSS feeds.
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class RSSStream extends Stream {
	
	public static NewsFeedSource SOURCE = NewsFeedSource.RSS;
	
	@Override
	public void open(StreamConfiguration config) {
		retriever = new RSSRetriever();
	}

	@Override
	public String getName() {
		return "RSS";
	}

}
