package gr.iti.mklab.sfc.streams;

import java.util.Date;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.common.domain.feeds.Feed;
import gr.iti.mklab.framework.retrievers.Response;
import gr.iti.mklab.framework.retrievers.Retriever;
import gr.iti.mklab.sfc.management.StorageHandler;
import gr.iti.mklab.sfc.streams.monitors.RateLimitsMonitor;

/**
 * Class responsible for handling the stream of information regarding 
 * a social network or a news feed source.
 * It is responsible for the configuration of the connection to the selected API
 * and the retrieval/storing of relevant content.
 * 
 * @author manosetro - manosetro@iti.gr
 *
 */
public abstract class Stream {

	protected static final String KEY = "Key";
	protected static final String SECRET = "Secret";
	protected static final String ACCESS_TOKEN = "AccessToken";
	protected static final String ACCESS_TOKEN_SECRET = "AccessTokenSecret";
	protected static final String CLIENT_ID = "ClientId";
	
	protected static final String MAX_REQUESTS = "maxRequests";
	protected static final String TIME_WINDOW = "timeWindow";
	
	// Default value 10 requests / minute
	protected int maxRequests = 10;
	protected long timeWindow = 1;
	
	//protected BlockingQueue<Feed> feedsQueue;
	protected Retriever retriever = null;
	protected StorageHandler storageHandler = null;
	
	protected RateLimitsMonitor rateLimitsMonitor;
	
	private Logger  logger = LogManager.getLogger(Stream.class);
	
	public abstract void open(Configuration config) throws StreamException;
	
	public void close() throws StreamException {
		if(retriever != null) {
			logger.info("Stop retriever");
		}
		
		logger.info("Close Stream  : " + this.getClass().getName());
	}
		
	public void setHandler(StorageHandler handler) {
		this.storageHandler = handler;
	}
	
	public StorageHandler getHandler() {
		return this.storageHandler;
	}
	
	public synchronized Response poll(Feed feed, int requests) throws StreamException {
		Response response = new Response(); 
		if(retriever != null) {
			
			if(feed == null) {
				logger.error("Feed is null in poll method.");
				return response;
			}
		
			try {
				response = retriever.retrieve(feed, requests);
				if(storageHandler != null) {
					for(Item item : response.getItems()) {
						storageHandler.handle(item);
					}
				}
				else {
					logger.error("Error: cannot store item. StorageHandler instance in null!");
				}
				
				if(response.getNumberOfItems() == 0) {
					logger.info("No items retrieved for (" + feed.getId() + ") since " + new Date(feed.getSinceDate()));
					return response;
				}
				
				logger.info("Feed: " + feed.getId() + " used " + response.getRequests() + " requests out of " + requests + ". " + response.getNumberOfItems() + " retrieved items since " + new Date(feed.getSinceDate())); 
				
				// Set new since date 
				if(feed.getSinceDate() < response.getLastTimestamp()) {
					feed.setSinceDate(response.getLastTimestamp());
					logger.info("New since date for (" + feed.getId() + "):  " +  new Date(feed.getSinceDate())); 
				}
			}
			catch(Exception e) {
				logger.error("Exception for feed " + feed.getId() + " of type " + feed.getClass(), e);
			}
		}
		else {
			throw new StreamException("Retriever is null for " + getName());
		}
		
		return response;
	}
	
	public synchronized Item poll(String id) throws StreamException {
		Item item = retriever.getItem(id);
		return item;
	}
	
	public abstract String getName();

	public int getMaxRequests() {
		return maxRequests;
	}
	
	public long getTimeWindow() {
		return timeWindow;
	}
	
}

