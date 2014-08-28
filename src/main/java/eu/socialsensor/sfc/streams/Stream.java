package eu.socialsensor.sfc.streams;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.StreamUser.Category;
import eu.socialsensor.framework.retrievers.Retriever;
import eu.socialsensor.sfc.streams.management.StorageHandler;
import eu.socialsensor.sfc.streams.monitors.FeedsMonitor;


/**
 * Class responsible for handling the stream of information regarding 
 * a social network or a news feed source.
 * It is responsible for the configuration of the connection to the selected API
 * and the retrieval/storing of relevant content.
 * @author manosetro
 * @email  manosetro@iti.gr
 * @author ailiakop
 * @email  ailiakop@iti.gr
 *
 */
public abstract class Stream implements Runnable {

	protected static final String KEY = "Key";
	protected static final String SECRET = "Secret";
	protected static final String ACCESS_TOKEN = "AccessToken";
	protected static final String ACCESS_TOKEN_SECRET = "AccessTokenSecret";
	protected static final String CLIENT_ID = "ClientId";
	protected static final String APP_ID = "AppId";
	protected static final String APP_SECRET = "AppSecret";
	
	protected static final String MAX_RESULTS = "maxResults";
	protected static final String MAX_REQUESTS = "maxRequests";
	protected static final String MAX_RUNNING_TIME = "maxRunningTime";
	
	protected FeedsMonitor monitor;
	protected BlockingQueue<Feed> feedsQueue;
	protected Retriever retriever = null;
	protected StorageHandler handler;
	
	protected List<Item> retrievedItems = new ArrayList<Item>();
	
	private Logger  logger = Logger.getLogger(Stream.class);
	
	private Map<String, Set<String>> usersToLists;
	private Map<String, Category> usersToCategory;
	
	/**
	 * Opens a stream for updates delivery
	 * @param config
	 *      Stream configuration parameters
	 * @throws StreamException
	 *      In any case of error during stream open
	 */
	public abstract void open(StreamConfiguration config) throws StreamException;
	
	/**
	 * Closes a stream 
	 * @throws StreamException
	 *      In any case of error during stream close
	 */
	public void close() throws StreamException {
	
		if(monitor != null) {
			logger.info("Stop monitor");
			monitor.stopMonitor();
		}
		
		if(retriever != null) {
			logger.info("Stop retriever");
			retriever.stop();
		}
		
		logger.info("Close Stream  : " + this.getClass().getName());
	}
		
	/**
	 * Sets the handler that is responsible for the handling 
	 * of the retrieved items
	 * @param handler
	 */
	public void setHandler(StorageHandler handler) {
		this.handler = handler;
	}
	
	/**
	 * Sets the feeds monitor for the stream
	 * @return
	 */
	public boolean setMonitor() {
		if(retriever == null)
			return false;
		
		monitor = new FeedsMonitor(retriever);
		return true;
	}
	/**
	 * Sets the users list that will be used to retrieve from the stream (utilized for Twitter Stream)
	 * @param usersToLists
	 */
	public void setUserLists(Map<String, Set<String>> usersToLists) {
		this.usersToLists = usersToLists;
		
		if(usersToLists != null) {
			Set<String> allLists = new HashSet<String>();
			for(Set<String> lists : usersToLists.values()) {
				allLists.addAll(lists);
			}
			logger.info("=============================================");
			logger.info(usersToLists.size() + " user in " + allLists.size() + " Lists!!!");
		}
	}
	/**
	 * Sets the category that the investigated user belongs to
	 * @param usersToCategory
	 */
	public void setUserCategories(Map<String, Category> usersToCategory) {
		this.usersToCategory = usersToCategory;
	}
	
	/**
	 * Returns the list of retrieved items 
	 * @return
	 */
	public synchronized List<Item> getRetrievedItems() {
		return retrievedItems;
	}
	
	/**
	 * Searches with the wrapper of the stream for a particular
	 * set of feeds (feeds can be keywordsFeeds, userFeeds, locationFeeds, listFeeds or URLFeeds)
	 * @param feeds
	 * @throws StreamException
	 */
	public synchronized void poll(List<Feed> feeds) throws StreamException {
		retrievedItems.clear();
		
		if(retriever != null) {
		
			if(feeds == null) {
				logger.error("Feeds is null in poll method.");
				return;
			}
			
			logger.info(getName() + ": poll for " + feeds.size() + " feeds");
			for(Feed feed : feeds) {
				try {
					List<Item> items = retriever.retrieve(feed);
					store(items);
					retrievedItems.addAll(items);
				}
				catch(Exception e) {
					logger.error("Exception for feed " + feed.getId() + " of type " + feed.getFeedtype());
					logger.error(e.getMessage());
				}
			}
			
			logger.info("Retrieved items for " + getName() + " are : " + retrievedItems.size());
		}
		
	}
	
	/**
	 * Store a set of items in the selected databases
	 * @param items
	 */
	public synchronized void store(List<Item>items) {
		for(Item item : items) {
			store(item);
		}
	}
	
	/**
	 * Store an item in the selected databases
	 * @param item
	 */
	public synchronized void store(Item item) {
		if(handler == null) {
			logger.error("NULL Handler!");
			return;
		}
			
		if(usersToLists != null && getUserList(item) != null)
			item.setList(getUserList(item));
		
		if(usersToCategory != null && getUserCategory(item) != null)
			item.setCategory(getUserCategory(item));
		
		handler.update(item);
	}
	
	/**
	 * Returns the lists that the user associated with a given 
	 * item belongs to
	 * @param item
	 * @return
	 */
	private String[] getUserList(Item item) {
		
		Set<String> lists = new HashSet<String>();
		if(usersToLists == null) {
			logger.error("User list is null");
			return null;
		}
			
		if(item.getUserId() == null) {
			logger.error("User in item is null");
			return null;
		}
				
		Set<String> userLists = usersToLists.get(item.getUserId());
		if(userLists != null) {
			lists.addAll(userLists);
		}
		
		for(String mention : item.getMentions()) {
			userLists = usersToLists.get(mention);
			if(userLists != null) {
				lists.addAll(userLists);
			}
		}
		
		String refUserId = item.getReferencedUserId();
		if(refUserId != null) {
			userLists = usersToLists.get(refUserId);
			if(userLists != null) {
				lists.addAll(userLists);
			}
		}
		
		if(lists.size() > 0) {
	
			return lists.toArray(new String[lists.size()]);
		}
		else {
	
			return null;
		}
		
	}
	/**
	 * Returns the category that a user associated with a given
	 * item belongs to
	 * @param item
	 * @return
	 */
	private Category getUserCategory(Item item) {
		
		if(usersToCategory == null){
			logger.error("User categories is null");
			return null;
		}
			
		if(item.getUserId() == null){
			logger.error("User in item is null");
			return null;
		}
			
		return usersToCategory.get(item.getUserId());
	}
	
	/**
	 * Deletes an item from the selected databases
	 * @param item
	 */
	public void delete(Item item) {
		handler.delete(item);
	}
	
	/**
	 * Adds a feed to the stream for future searching
	 * @param feed
	 * @return
	 */
	public boolean addFeed(Feed feed) {
		if(feedsQueue == null)
			return false;
		
		return feedsQueue.offer(feed);
	}
	/**
	 * Adds a set of feeds to the stream for future searching
	 * @param feeds
	 * @return
	 */
	public boolean addFeeds(List<Feed> feeds) {
		for(Feed feed : feeds) {
			if(!addFeed(feed))
				return false;
		}
		return true;
	}
	
	
	@Override
	public void run() {
		while(true) {
			try {
				Feed feed = feedsQueue.take();
				monitor.addFeed(feed);
				monitor.startMonitor(feed);
				
			} catch (InterruptedException e) {
				logger.error(e);
				return;
			}
		}
	}
	
	public abstract String getName();
	
}
