package gr.iti.mklab.sfc;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.xml.sax.SAXException;

import gr.iti.mklab.framework.common.domain.collections.Collection;
import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.common.domain.feeds.Feed;
import gr.iti.mklab.sfc.input.CollectionsManager;
import gr.iti.mklab.sfc.management.StorageHandler;
import gr.iti.mklab.sfc.streams.Stream;
import gr.iti.mklab.sfc.streams.StreamException;
import gr.iti.mklab.sfc.streams.StreamsManagerConfiguration;
import gr.iti.mklab.sfc.streams.monitors.ItemsMonitor;
import gr.iti.mklab.sfc.streams.monitors.StreamsMonitor;
import gr.iti.mklab.sfc.subscribers.Subscriber;

/**
 * Class for retrieving content according to  keywords - user - location feeds from social networks.
 * Currently 7 social networks are supported (Twitter,Youtube,Facebook,Flickr,Instagram,Tumblr,GooglePlus)
 * 
 * @author Manos Schinas - manosetro@iti.gr
 * 
 */
public class StreamsManager implements Runnable {
	
	public final Logger logger = LogManager.getLogger(StreamsManager.class);
	
	enum ManagerState {
		OPEN, CLOSE
	}

	private String redisHost = null;

	private Map<String, Stream> streams = null;
	private Map<String, Subscriber> subscribers = null;
	
	private StreamsManagerConfiguration config = null;
	private StorageHandler storageHandler;
	
	private StreamsMonitor monitor = null;
	
	private ManagerState state = ManagerState.CLOSE;

	private BlockingQueue<Pair<Collection, String>> cQueue = new LinkedBlockingQueue<Pair<Collection, String>>();
	private BlockingQueue<Pair<Pair<String, String>, String>> itemsQueue = new LinkedBlockingQueue<Pair<Pair<String, String>, String>>();
	
	private CollectionsManager collectionsManager;
	
	private Map<Feed, Integer> feeds = new HashMap<Feed, Integer>();
	private Map<Feed, Set<String>> collectionsPerFeed = new HashMap<Feed, Set<String>>();
	private Map<String, Long> collectionsStatus = new HashMap<String, Long>();
	
	private RedisSubscriber jedisPubSub;

	private ItemsMonitor itemsMonitor = new ItemsMonitor(itemsQueue);

	private Thread thisThread = null;
	
	public StreamsManager(StreamsManagerConfiguration config) throws StreamException {

		if (config == null) {
			logger.error("Config file in null.");
			throw new StreamException("Manager's configuration must be specified");
		}
		
		//Set the configuration files
		this.config = config;
		
		//Set up the Subscribers
		initSubscribers();
		
		//Set up the Streams
		initStreams();
	}
	
	/**
	 * Opens Manager by starting the auxiliary modules and setting up
	 * the database for reading/storing
	 * 
	 * @throws StreamException Stream Exception
	 */
	public synchronized void open() throws StreamException {
		
		if (state == ManagerState.OPEN) {
			logger.error("Stream manager is already open.");
			return;
		}
		
		state = ManagerState.OPEN;
		logger.info("StreamsManager is open.");
		try {
			Configuration inputConfig = config.getInputConfig();
			
			redisHost = inputConfig.getParameter("redis.host", "127.0.0.1");
			
			//Start stream handler 
			storageHandler = new StorageHandler(config);
			storageHandler.start();	
			logger.info("Storage Manager is ready to store.");
			
			collectionsManager = new CollectionsManager(inputConfig);
			
			//Start the Subscribers
			Map<String, Set<Feed>> feedsPerSource =  collectionsManager.createFeedsPerSource();
			for(String subscriberId : subscribers.keySet()) {
				logger.info("Stream Manager - Start Subscriber : " + subscriberId);
				Configuration srconfig = config.getSubscriberConfig(subscriberId);
				Subscriber subscriber = subscribers.get(subscriberId);
				
				subscriber.setHandler(storageHandler);
				subscriber.open(srconfig);
				
				Set<Feed> sourceFeed = feedsPerSource.get(subscriberId);
				subscriber.subscribe(sourceFeed);
			}
			
			//Start the Streams
			//If there are Streams to monitor start the StreamsMonitor
			if(streams != null && !streams.isEmpty()) {
				monitor = new StreamsMonitor(streams.size());
				for (String streamId : streams.keySet()) {
					logger.info("Start Stream : " + streamId);
					
					Configuration sconfig = config.getStreamConfig(streamId);
					Stream stream = streams.get(streamId);
					stream.setHandler(storageHandler);
					stream.open(sconfig);
				
					monitor.addStream(stream);
				}
				monitor.start();
			}
			else {
				logger.error("There are no streams to open.");
			}
			
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new StreamException("Error during streams open", e);
		}
	}
	
	/**
	 * Closes Manager and its auxiliary modules
	 * 
	 * @throws StreamException Stream Exception
	 */
	public synchronized void close() throws StreamException {
		
		if (state == ManagerState.CLOSE) {
			logger.info("StreamManager is already closed.");
			return;
		}
		
		try {
			for (Stream stream : streams.values()) {
				logger.info("Close " + stream);
				stream.close();
			}
			
			itemsMonitor.stop();
			
			if (storageHandler != null) {
				storageHandler.stop();
			}
			
			jedisPubSub.close();
			
			state = ManagerState.CLOSE;
		}
		catch(Exception e) {
			throw new StreamException("Error during streams close", e);
		}
	}
	
	/**
	 * Initializes the streams apis that are going to be searched for relevant content
	 * 
	 * @throws StreamException Stream Exception
	 */
	private void initStreams() throws StreamException {
		streams = new HashMap<String, Stream>();
		try {
			for (String streamId : config.getStreamIds()) {
				Configuration sconfig = config.getStreamConfig(streamId);
				Stream stream = (Stream)Class.forName(sconfig.getParameter(Configuration.CLASS_PATH)).newInstance();
				logger.info("Init " + streamId + ". Max Requests: " +stream.getMaxRequests() + " per " + stream.getTimeWindow());
				streams.put(streamId, stream);
			}
		}catch(Exception e) {
			e.printStackTrace();
			throw new StreamException("Error during streams initialization", e);
		}
	}
	
	/**
	 * Initializes the streams apis, that implement subscriber channels, that are going to be searched for relevant content
	 * 
	 * @throws StreamException Stream Exception
	 */
	private void initSubscribers() throws StreamException {
		subscribers = new HashMap<String, Subscriber>();
		try {
			for (String subscriberId : config.getSubscriberIds()) {
				Configuration sconfig = config.getSubscriberConfig(subscriberId);
				Subscriber subscriber = (Subscriber) Class.forName(sconfig.getParameter(Configuration.CLASS_PATH)).newInstance();
				subscribers.put(subscriberId, subscriber);
			}
		} 
		catch(Exception e) {
			e.printStackTrace();
			throw new StreamException("Error during Subscribers initialization", e);
		}
	}
	
	@Override
	public void run() {

		if(state != ManagerState.OPEN) {
			logger.error("Streams Manager is not open!");
			return;
		}
		
		Map<String, Collection> collections = collectionsManager.getActiveCollections();
		logger.info(collections.size() + " active collections in db.");
		for(Collection collection : collections.values()) {
			try {
				cQueue.put(Pair.of(collection, "collections:new"));
			} catch (InterruptedException e) {
				logger.error(e);
			}
		}
		
		itemsMonitor.addFetchTasks(monitor.getStreamFetchTasks());
		itemsMonitor.start();
		
		jedisPubSub = new RedisSubscriber(cQueue, itemsQueue, redisHost);
		jedisPubSub.start();
		
		logger.info("Start to monitor for updates on collections.");
		while(state == ManagerState.OPEN) {
			try {
				
				Pair<Collection, String> actionPair = cQueue.take();
				if(actionPair == null) {
					logger.error("Received action pair is null.");
					continue;
				}
				
				Collection collection = actionPair.getKey();
				String action = actionPair.getRight();
				logger.info("Action: " + action + " - collection: " + collection.getId() + " from user " + collection.geOwnertId());
				
				if(monitor == null) {
					logger.error("Monitor has not been initialized. Cannot monitor any feed.");
				}
				
				switch (action) {
    				case "collections:new":
    					List<Feed> feedsToInsert = collection.getFeeds();
    					logger.info(feedsToInsert.size() + " feeds to insert");
    					for(Feed feed : feedsToInsert) {
    						
    						logger.info("Insert: " + feed);
    						
    						String feedId = feed.getId();
							String streamId = feed.getSource();
							Stream stream = monitor.getStream(streamId);
							
							if(stream == null) {
								logger.error("Stream " + streamId + " has not initialized. Feed " + feed + " cannot be added.");
							}
							else {
								Integer count = feeds.get(feed);
								if(count != null) {
									feeds.put(feed, ++count);
									logger.info("Feed " + feed + " is already under monitoring. Increase priority: " + count);
								}
								else {
									// Add to monitors
									logger.info("Add " + feed + " to " + streamId);
									feeds.put(feed, 1);
									monitor.addFeed(streamId, feed);
								}
								
								// keep track of feeds - collections 
								Set<String> collectionsSet = collectionsPerFeed.get(feedId);
	    						if(collectionsSet == null) {
	    							collectionsSet = new HashSet<String>();
	    							collectionsPerFeed.put(feed, collectionsSet);
	    						}
	    						collectionsSet.add(collection.getId());
							}	
    					}
    					continue;
    					
    				case "collections:stop":
    				case "collections:delete":
    					List<Feed> feedsToDelete = collection.getFeeds();
    					logger.info(feedsToDelete.size() + " feeds to stop/delete");
    					for(Feed feed : feedsToDelete) {
    						String feedId = feed.getId();
    						String streamId = feed.getSource();
							Stream stream = monitor.getStream(streamId);
							if(stream == null) {
								logger.error("Stream " + streamId + " has not initialized. Feed " + feed + " cannot be removed!");
							}
							else {
								Integer count = feeds.get(feed);
								if(count != null) {
									if(count > 1) {
										feeds.put(feed, --count);
										logger.info("Feed " + feed + " priority decreased to " + count);
									}
									else {
										// Remove from monitors
										logger.info("Remove " + feed + " from " + streamId);
										feeds.remove(feed);
										monitor.removeFeed(streamId, feed);
									}
								}
								else {
									logger.info("Feed " + feed + " does not exist. Cannot remove it");
								}
								
								// keep track of feeds - collections 
	    						Set<String> collectionsSet = collectionsPerFeed.get(feedId);
	    						if(collectionsSet != null) {
	    							collectionsSet.remove(collection.getId());
	    							if(collectionsSet.isEmpty()) {
	    								collectionsPerFeed.remove(feedId);
	    							}
	    						}
							}
   
    					}
    					continue;
    					
    				default:
    					logger.error("Unrecognized action: " + action);
				}
			} catch (InterruptedException e) {
				logger.error("InterruptedException => " + e.getMessage());
			} catch (Exception e) {
				logger.error("Exception => " + e.getMessage());
			}
		}
		logger.info("Exit from stream manager's run loop.");
	}
	
	public void start() {
		thisThread = new Thread(this);
		thisThread.start();
	}
	
	public boolean isRunning() {
		if(thisThread == null) {
			return false;
		}
		
		if(!thisThread.isAlive()) {
			return false;
		}
		
		return true;
	}
	
	public static void main(String[] args) {
		
		Logger logger = LogManager.getLogger(StreamsManager.class);
		
		File streamConfigFile;
		if(args.length != 1 ) {
			streamConfigFile = new File("./conf/streams.conf.xml");
		}
		else {
			streamConfigFile = new File(args[0]);
		}
		
		StreamsManager manager = null;
		try {
			StreamsManagerConfiguration config = StreamsManagerConfiguration.readFromFile(streamConfigFile);		
	        
			manager = new StreamsManager(config);
			manager.open();
			
			Runtime.getRuntime().addShutdownHook(new Shutdown(manager));
			
			manager.start();
			
		} catch (ParserConfigurationException e) {
			logger.error(e);
		} catch (SAXException e) {
			logger.error(e);
		} catch (IOException e) {
			logger.error(e);
		} catch (StreamException e) {
			logger.error(e);
		} catch (Exception e) {
			logger.error(e);
		}	
		
		if(manager == null) {
			logger.error("");
			System.exit(-1);
		}
		
		logger.info("Stream manager initialized!");
		while(manager.state == ManagerState.OPEN) {
			
			if(!manager.isRunning()) {
				logger.error("Stream Manager main thread is not running. Restart it.");
				manager.start();
			}
			
			ThreadContext.put("id", UUID.randomUUID().toString());
			ThreadContext.put("date", new Date().toString());
			List<String> fIds = new ArrayList<String>();
			for(Entry<Feed, Set<String>> entry : manager.collectionsPerFeed.entrySet()) {
				Feed feed = entry.getKey();
				fIds.add(feed.getId());
				
				long until = feed.getUntilDate();
				Set<String> collections = entry.getValue();
				for(String cId : collections) {
					Long lastRunningTime = manager.collectionsStatus.get(cId);
					if(lastRunningTime == null || lastRunningTime < until) {
						manager.collectionsStatus.put(cId, until);
					}
				}
				logger.info("Feed [" + feed.getId() + "] is under supervision - Collections: " + collections);
			}
			logger.info("Active Feeds: " + fIds + ". Timestamp: " + new Date());
			
			Map<String, Collection> storedRunningCollections = manager.collectionsManager.getActiveCollections();			
			Set<String> cIds = new HashSet<String>(storedRunningCollections.keySet());
			cIds.removeAll(manager.collectionsStatus.keySet());
			
			logger.error("Monitoring check: " + cIds.size() + " collections are missing (" + cIds + "). Re-insert for monitoring.");
			for(String cId : cIds) {
				try {
					Collection collection = storedRunningCollections.get(cId);
					manager.cQueue.put(Pair.of(collection, "collections:new"));
				} catch (InterruptedException e) {
					logger.error(e);
				}
				
			}
			ThreadContext.clearAll();
			
			try {
				Thread.sleep(300000);
			} catch (InterruptedException e) {
				logger.error(e);
			}
		}
	}
	
	
	
}
