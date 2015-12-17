package gr.iti.mklab.sfc.streams.monitors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import gr.iti.mklab.framework.common.domain.feeds.Feed;
import gr.iti.mklab.framework.retrievers.Response;
import gr.iti.mklab.sfc.streams.Stream;


/**
 * Class for handling a Stream Task that is responsible for retrieving
 * content for the stream it is assigned to.
 * 
 * @author Manos Schinas
 * @email  manosetro@iti.gr
 */
public class StreamFetchTask implements  Callable<Integer>, Runnable {
	
	private final Logger logger = LogManager.getLogger(StreamFetchTask.class);
	
	private Stream stream;
	
	private Map<String, FeedFetch> feeds = Collections.synchronizedMap(new HashMap<String, FeedFetch>());
	private LinkedBlockingQueue<Feed> feedsQueue = new LinkedBlockingQueue<Feed>();
	
	private int maxRequests;
	private long period;
	
	private long totalRetrievedItems = 0;

	private AtomicInteger requests = new AtomicInteger(0);
	private long lastResetTime = 0l;
	
	private long lastExecutionTime = 0l;
	private String lastExecutionFeed = null;
	
	public StreamFetchTask(Stream stream) throws Exception {
		this.stream = stream;
		
		this.maxRequests = stream.getMaxRequests();
		this.period = stream.getTimeWindow() * 60000;	
	}
	
	/**
	 * Adds the input feeds to search for relevant content in the stream
	 * 
	 * @param Feed feed
	 */
	public void addFeed(Feed feed) {
		FeedFetch feedFetch = new FeedFetch(feed);
		this.feeds.put(feed.getId(), feedFetch);
		this.feedsQueue.offer(feed);
	}
	
	/**
	 * Adds the input feeds to search for relevant content in the stream
	 * 
	 * @param List<Feed> feeds
	 */
	public void addFeeds(List<Feed> feeds) {
		for(Feed feed : feeds) {
			addFeed(feed);
		}
	}
	
	/**
	 * Remove input feed from task
	 * 
	 * @param Feed feed
	 */
	public void removeFeed(Feed feed) {
		this.feeds.remove(feed.getId());
		this.feedsQueue.remove(feed);
	}

	/**
	 * Remove input feed from task
	 * 
	 * @param Feed feed
	 */
	public void removeFeeds(List<Feed> feeds) {
		for(Feed feed : feeds) {
			removeFeed(feed);
		}
	}
	
	
	public long getTotalRetrievedItems() {
		return totalRetrievedItems;
	}

	public void setTotalRetrievedItems(long totalRetrievedItems) {
		this.totalRetrievedItems = totalRetrievedItems;
	}
	
	public Date getLastExecutionTime() {
		return new Date(lastExecutionTime);
	}

	public String getLastExecutionFeed() {
		return lastExecutionFeed;
	}
	
	public List<Feed> getFeedsToPoll() {
		List<Feed> feedsToPoll = new ArrayList<Feed>();
		long currentTime = System.currentTimeMillis();
		// Check for new feeds
		for(FeedFetch feedFetch : feeds.values()) {
			// each feed can run one time in each period
			if((currentTime - feedFetch.getLastExecution()) > period) { 
				feedsToPoll.add(feedFetch.getFeed());
			}
		}
		return feedsToPoll;
	}
	
	/**
	 * Retrieves content using the feeds assigned to the task
	 * making rest calls to stream's API. 
	 */
	@Override
	public Integer call() throws Exception {
		int totalItems = 0;
		try {
			long currentTime = System.currentTimeMillis();
			if((currentTime -  lastResetTime) > period) {
				logger.info("Reset available requests for " + stream.getName());
				
				requests.set(0);	// reset performed requests
				lastResetTime = currentTime;
			}

			// get feeds ready for polling
			List<Feed> feedsToPoll = getFeedsToPoll();
			
			if(!feedsToPoll.isEmpty()) {
				int numOfFeeds = feedsToPoll.size();
				int remainingRequests = (maxRequests - requests.get()) / numOfFeeds;	// remaining requests per feed
				if(remainingRequests < 1) {
					logger.info("Remaining Requests: " + remainingRequests + " for " + stream.getName());
					return totalItems;
				}
				
				for(Feed feed : feedsToPoll) {
					logger.info("Poll for " + feed);
					
					long executionTime = System.currentTimeMillis();
					
					Response response = stream.poll(feed, remainingRequests);
					totalItems += response.getNumberOfItems();
					
					lastExecutionTime = System.currentTimeMillis();
					lastExecutionFeed = feed.getId();
					
					// increment performed requests
					requests.addAndGet(response.getRequests());
					
					FeedFetch feedFetch = feeds.get(feed.getId());
					if(feedFetch != null) {
						feedFetch.setLastExecution(executionTime);
						feedFetch.incFetchedItems(response.getNumberOfItems());
					}
					else {
						logger.error("There is no fetch structure for feed (" + feed.getId() + ")");
					}
					
				}
				return totalItems;
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception in stream fetch task for " + stream.getName(), e);
		}	
		return totalItems;
	}

	@Override
	public void run() {
		int maxRequestsPerFeed = (int) (0.2 * maxRequests);
		
		while(true) {
			try {
				long currentTime = System.currentTimeMillis();
				if((currentTime -  lastResetTime) > period) {
					logger.info((maxRequests - requests.get()) + " available requests for " + stream.getName() + ". Reset them to " + maxRequests);
					
					requests.set(0);	// reset performed requests
					lastResetTime = currentTime;
				}

				// get feeds ready for polling
				Feed feed = feedsQueue.take();
				feedsQueue.offer(feed);
				
				List<Feed> feedsToPoll = getFeedsToPoll();
				if(!feedsToPoll.isEmpty()) {
					if(feedsToPoll.contains(feed)) {
						int requestsPerFeed = Math.min(maxRequestsPerFeed, (maxRequests - requests.get()) / feedsToPoll.size());
						if(requestsPerFeed < 1) {
							logger.info("No more remaining requests for " + stream.getName());
							logger.info("Wait for " + (currentTime -  lastResetTime - period)/1000 + " seconds until reseting.");
						}
						else {
							logger.info("Poll for [" + feed.getId() + "]. Requests: " + requestsPerFeed);
							
							Response response = stream.poll(feed, requestsPerFeed);
							totalRetrievedItems += response.getNumberOfItems();
						
							lastExecutionTime = System.currentTimeMillis();
							lastExecutionFeed = feed.getId();
							
							// increment performed requests
							requests.addAndGet(response.getRequests());
							
							FeedFetch feedFetch = feeds.get(feed.getId());
							if(feedFetch != null) {
								feedFetch.setLastExecution(lastExecutionTime);
								feedFetch.incFetchedItems(response.getNumberOfItems());
							}
							else {
								logger.error("There is no fetch structure for feed (" + feed.getId() + ")");
							}

						}
					}
				}
				else {
					Thread.sleep(2000);
				}				
			} catch (Exception e) {
				logger.error("Exception in stream fetch task for " + stream.getName(), e);
			}	
		}
	}
	
	public class FeedFetch {
		
		private Feed feed;
		private Long lastExecution = 0L;
		
		private Long fetchedItems = 0L;
		
		public FeedFetch(Feed feed) {
			this.feed = feed;
		}

		public Long getLastExecution() {
			return lastExecution;
		}

		public void setLastExecution(Long lastExecution) {
			this.lastExecution = lastExecution;
		}

		public Feed getFeed() {
			return feed;
		}

		public void setFeed(Feed feed) {
			this.feed = feed;
		}

		public Long getFetchedItems() {
			return fetchedItems;
		}

		public void setFetchedItems(Long fetchedItems) {
			this.fetchedItems = fetchedItems;
		}
		
		public void incFetchedItems(Integer fetchedItems) {
			this.fetchedItems += fetchedItems;
		}
	}
	
}
