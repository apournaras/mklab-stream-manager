package gr.iti.mklab.sfc.streams.impl;

import gr.iti.mklab.framework.common.domain.NewsFeedSource;
import gr.iti.mklab.framework.retrievers.impl.RSSRetriever;
import gr.iti.mklab.sfc.streams.Stream;
import gr.iti.mklab.sfc.streams.StreamConfiguration;

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
