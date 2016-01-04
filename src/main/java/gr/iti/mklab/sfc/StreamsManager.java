package gr.iti.mklab.sfc;

import java.io.File;
import java.io.IOException;
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
import org.mongodb.morphia.Morphia;
import org.xml.sax.SAXException;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import gr.iti.mklab.framework.common.domain.collections.Collection;
import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.common.domain.feeds.Feed;
import gr.iti.mklab.sfc.input.CollectionsManager;
import gr.iti.mklab.sfc.management.StorageHandler;
import gr.iti.mklab.sfc.streams.Stream;
import gr.iti.mklab.sfc.streams.StreamException;
import gr.iti.mklab.sfc.streams.StreamsManagerConfiguration;
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

	private Jedis jedis = null;
	
	private Map<String, Stream> streams = null;
	private Map<String, Subscriber> subscribers = null;
	
	private StreamsManagerConfiguration config = null;
	private StorageHandler storageHandler;
	
	private StreamsMonitor monitor = null;
	
	private ManagerState state = ManagerState.CLOSE;

	private BlockingQueue<Pair<Collection, String>> queue = new LinkedBlockingQueue<Pair<Collection, String>>();
	
	private CollectionsManager collectionsManager;
	private Map<Feed, Integer> feeds = new HashMap<Feed, Integer>();
	private Map<String, Set<String>> collectionsPerFeed = new HashMap<String, Set<String>>();
	
	private RedisSubscriber jedisPubSub;

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
			String redisHost = inputConfig.getParameter("redis.host", "127.0.0.1");
			jedis = new Jedis(redisHost);
			
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
			
			if (storageHandler != null) {
				storageHandler.stop();
			}
			
			jedisPubSub.unsubscribe();
			
			state = ManagerState.CLOSE;
		}
		catch(Exception e) {
			throw new StreamException("Error during streams close", e);
		}
	}
	
	/**
	 * Initializes the streams apis that are going to be searched for 
	 * relevant content
	 * 
	 * @throws StreamException Stream Exception
	 */
	private void initStreams() throws StreamException {
		streams = new HashMap<String, Stream>();
		try {
			for (String streamId : config.getStreamIds()) {
				Configuration sconfig = config.getStreamConfig(streamId);
				Stream stream = (Stream)Class.forName(sconfig.getParameter(Configuration.CLASS_PATH)).newInstance();
				streams.put(streamId, stream);
			}
		}catch(Exception e) {
			e.printStackTrace();
			throw new StreamException("Error during streams initialization", e);
		}
	}
	
	/**
	 * Initializes the streams apis, that implement subscriber channels, that are going to be searched for 
	 * relevant content
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
				queue.put(Pair.of(collection, "collections:new"));
			} catch (InterruptedException e) {
				logger.error(e);
			}
		}
		
		jedisPubSub = new RedisSubscriber(queue);
		Thread redisThread = new Thread(jedisPubSub);
		redisThread.start();
		
		logger.info("Start to monitor for updates on collections.");
		while(state == ManagerState.OPEN) {
			try {
				
				Pair<Collection, String> actionPair = queue.take();
				if(actionPair == null) {
					continue;
				}
				
				Collection collection = actionPair.getKey();
				String action = actionPair.getRight();
				logger.info("Action: " + action + " - collection: " + collection.getId() + " from user " + collection.geOwnertId());
				
				if(monitor == null) {
					logger.error("Monitor is null. Cannot monitor any feed.");
				}
				
				switch (action) {
    				case "collections:new":
    					List<Feed> feedsToInsert = collection.getFeeds();
    					logger.info(feedsToInsert.size() + " feeds to insert");
    					for(Feed feed : feedsToInsert) {
    						
    						String feedId = feed.getId();
							String streamId = feed.getSource();
							Stream stream = monitor.getStream(streamId);
							
							if(stream == null) {
								logger.error("Stream " + streamId + " has not initialized.");
								logger.error("Feed (" + feedId + ") of type " + feed.getSource() + " cannot be added.");
							}
							else {
								logger.info("Feed to insert: [" + feedId + "] in " + streamId + " monitor");
								Integer count = feeds.get(feed);
								if(count != null) {
									feeds.put(feed, ++count);
									logger.info("Feed (" + feedId + ") is already under monitoring. Increase priority: " + count);
								}
								else {
									feeds.put(feed, 1);
									// Add to monitors
									if(stream != null) { 
										logger.info("Add (" + feedId + ") to " + streamId);
										monitor.addFeed(streamId, feed);
									}
								}
								
								Set<String> collectionsSet = collectionsPerFeed.get(feedId);
	    						if(collectionsSet == null) {
	    							collectionsSet = new HashSet<String>();
	    							collectionsPerFeed.put(feedId, collectionsSet);
	    						}
	    						collectionsSet.add(collection.getId());
							}	
    					}
    					break;
    				case "collections:stop":
    				case "collections:delete":
    					List<Feed> feedsToDelete = collection.getFeeds();
    					logger.info(feedsToDelete.size() + " feeds to delete");
    					for(Feed feed : feedsToDelete) {
    						String feedId = feed.getId();
    						String streamId = feed.getSource();
							Stream stream = monitor.getStream(streamId);
							if(stream == null) {
								logger.error("Stream " + streamId + " has not initialized.");
								logger.error("Feed (" + feedId + ") of type " + feed.getSource() + " cannot be removed!");
							}
							else {
								Integer count = feeds.get(feed);
								if(count != null) {
									if(count > 1) {
										feeds.put(feed, --count);
										logger.info("Feed (" + feedId + ") priority decreased to " + count);
									}
									else {
										feeds.remove(feed);
										// Remove from monitors
										logger.info("Remove (" + feedId + ") from " + streamId);
										monitor.removeFeed(streamId, feed);
    		
									}
								}
	    						Set<String> collectionsSet = collectionsPerFeed.get(feedId);
	    						if(collectionsSet != null) {
	    							collectionsSet.remove(collection.getId());
	    							if(collectionsSet.isEmpty()) {
	    								collectionsPerFeed.remove(feedId);
	    							}
	    						}
							}
   
    					}
    					break;
    				default:
    					logger.error("Unrecognized action");
				}
			} catch (InterruptedException e) {
				logger.error("Exception: " + e.getMessage());
			}
		}
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
			
			Thread thread = new Thread(manager);
			thread.start();
			
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
		
		logger.info("Stream manager initialized!");
		while(manager.state == ManagerState.OPEN) {
			ThreadContext.put("id", UUID.randomUUID().toString());
			ThreadContext.put("date", new Date().toString());
			for(Entry<String, Set<String>> entry : manager.collectionsPerFeed.entrySet()) {
				logger.info("Feed [" + entry.getKey() + "] - Collections: " + entry.getValue());
			}
			ThreadContext.clearAll();
			
			try {
				Thread.sleep(300000);
			} catch (InterruptedException e) {
				logger.error(e);
			}
		}
		
	}
	
	public class RedisSubscriber extends JedisPubSub implements Runnable {

		private Logger logger = LogManager.getLogger(RedisSubscriber.class);
		private Morphia morphia = new Morphia();
		
		private BlockingQueue<Pair<Collection, String>> queue;
		
		public RedisSubscriber(BlockingQueue<Pair<Collection, String>> queue) {
			this.queue = queue;
			this.morphia.map(Collection.class);
		}
		
	    @Override
	    public void onMessage(String channel, String message) {
	    	
	    }

	    @Override
	    public void onPMessage(String pattern, String channel, String message) {	
	    	try {
	    		logger.info("Message received: " + message);
	    		logger.info("Pattern: " + pattern + ", Channel: " + channel);
	    		
	    		DBObject obj = (DBObject) JSON.parse(message);	    	
	    		Collection collection = morphia.fromDBObject(Collection.class, obj);
	    		
	    		Pair<Collection, String> actionPair = Pair.of(collection, channel);
				queue.put(actionPair);
				
	    	}
	    	catch(Exception e) {
	    		logger.error("Error on message " + message + ". Channel: " + channel + " pattern: " + pattern, e);
	    	}
	    }

	    @Override
	    public void onSubscribe(String channel, int subscribedChannels) {
	    	logger.info("Subscribe to " + channel + " channel. " + subscribedChannels + " active subscriptions.");
	    }

	    @Override
	    public void onUnsubscribe(String channel, int subscribedChannels) {
	    	logger.info("Unsubscribe from " + channel + " channel. " + subscribedChannels + " active subscriptions.");
	    }

	    @Override
	    public void onPUnsubscribe(String pattern, int subscribedChannels) {
	    	logger.info("Unsubscribe to " + pattern + " pattern. " + subscribedChannels + " active subscriptions.");
	    }

	    @Override
	    public void onPSubscribe(String pattern, int subscribedChannels) {
	    	logger.info("Subscribe from " + pattern + " pattern. " + subscribedChannels + " active subscriptions.");
	    }

		@Override
		public void run() {
			logger.info("Subscribe to channel collections:*");
			jedis.psubscribe(this, "collections:*");
		}
	}
	
}
