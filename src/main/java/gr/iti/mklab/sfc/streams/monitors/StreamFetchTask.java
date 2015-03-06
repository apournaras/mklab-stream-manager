package gr.iti.mklab.sfc.streams.monitors;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.feeds.Feed;
import gr.iti.mklab.sfc.streams.Stream;


/**
 * Class for handling a Stream Task that is responsible for retrieving
 * content for the stream it is assigned to.
 * 
 * @author Manos Schinas
 * @email  manosetro@iti.gr
 */
public class StreamFetchTask implements  Callable<Integer> {
	
	private final Logger logger = Logger.getLogger(StreamFetchTask.class);
	
	private Stream stream;
	
	private Set<Feed> feeds = new HashSet<Feed>();
	
	private long lastRuntime = 0;
	
	public StreamFetchTask(Stream stream) throws Exception {
		this.stream = stream;
	}
	
	public StreamFetchTask(Stream stream, Set<Feed> feeds) throws Exception {
		this.stream = stream;
		this.feeds.addAll(feeds);
	}
	
	/**
	 * Adds the input feeds to search for relevant content in the stream
	 * 
	 * @param Feed feed
	 */
	public void addFeed(Feed feed) {
		this.feeds.add(feed);
	}
	
	/**
	 * Adds the input feeds to search for relevant content in the stream
	 * 
	 * @param Feed feed
	 */
	public void removeFeed(Feed feed) {
		this.feeds.remove(feed);
	}
	
	/**
	 * Adds the input feeds to search for relevant content in the stream
	 * 
	 * @param List<Feed> feeds
	 */
	public void addFeeds(List<Feed> feeds) {
		this.feeds.addAll(feeds);
	}

	public long getLastRuntime() {
		return this.lastRuntime;
	}
	
	/**
	 * Retrieves content using the feeds assigned to the task
	 * making rest calls to stream's API. 
	 */
	@Override
	public Integer call() throws Exception {
		try {
			if(!feeds.isEmpty()) {
				logger.info("Poll " + feeds.size() + " feeds in " + stream.getName());
				List<Item> items = stream.poll(feeds);
				lastRuntime = System.currentTimeMillis();
				
				return items.size();
			}
			else {
				logger.info("Feeds are empty for " + stream.getName());
			}
		} catch (Exception e) {
			logger.error("ERROR IN STREAM FETCH TASK: " + e.getMessage());
			
		}	
		return 0;
	}
}
