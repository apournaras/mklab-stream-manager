package gr.iti.mklab.sfc.streams.impl;

import java.util.Date;

import com.restfb.util.StringUtils;

import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.Source;
import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.common.domain.feeds.RssFeed;
import gr.iti.mklab.framework.retrievers.Response;
import gr.iti.mklab.framework.retrievers.impl.RssRetriever;
import gr.iti.mklab.sfc.streams.Stream;

/**
 * Class responsible for setting up the connection for retrieving RSS feeds.
 * 
 * @author Manos Schinas
 * 
 * @email  manosetro@iti.gr
 */
public class RssStream extends Stream {
	
	public static Source SOURCE = Source.RSS;
	
	@Override
	public void open(Configuration config) {
		retriever = new RssRetriever();
	}

	@Override
	public String getName() {
		return SOURCE.name();
	}

	public static void main(String...args) throws Exception {
		
		String id = "ft_elec";
		String url = "http://www.spectator.co.uk/tag/general-election-2015/feed";		
		String source = "RSS";
		long since = System.currentTimeMillis() - 25*24*3600*1000L;
		
		RssFeed feed = new RssFeed(id, url, since, source);
			
		RssRetriever retriever = new RssRetriever();
		Response response = retriever.retrieve(feed);
		
		System.out.println(response.getNumberOfItems());
		for(Item item : response.getItems()) {
			System.out.println("ID: " + item.getId());
			System.out.println("Title: " + item.getTitle());
			System.out.println(new Date(item.getPublicationTime()));
			System.out.println(item.getDescription());
			System.out.println("User: " + item.getUserId());
			System.out.println("Url: " + item.getUrl());
			System.out.println("Tags: " + StringUtils.join(item.getTags()));
			System.out.println(item.getMediaItems());
			System.out.println("Comments: " + item.getComments());
			System.out.println("====================================");
		}
	}
}
