package eu.socialsensor.sfc.streams.management;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import eu.socialsensor.framework.client.search.solr.SolrDyscoHandler;
import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.Keyword;
import eu.socialsensor.framework.common.domain.Query;
import eu.socialsensor.framework.common.domain.dysco.Dysco;
import eu.socialsensor.framework.common.domain.dysco.Dysco.DyscoType;
import eu.socialsensor.framework.common.domain.dysco.Message;
import eu.socialsensor.framework.common.domain.dysco.Message.Action;
import eu.socialsensor.framework.common.domain.feeds.KeywordsFeed;
import eu.socialsensor.framework.streams.Stream;
import eu.socialsensor.framework.streams.StreamConfiguration;
import eu.socialsensor.framework.streams.StreamException;
import eu.socialsensor.sfc.builder.FeedsCreator;
import eu.socialsensor.sfc.builder.SolrQueryBuilder;
import eu.socialsensor.sfc.builder.input.DataInputType;
import eu.socialsensor.sfc.streams.StreamsManagerConfiguration;
import eu.socialsensor.sfc.streams.monitors.StreamsMonitor;

/**
 * Class responsible for searching media content in social networks
 * (Twitter, YouTube, Facebook, Google+, Instagram, Flickr, Tumblr)
 * give a DySco as input. The retrieval of relevant content is based
 * on queries embedded to the DySco. DyScos are received as messages 
 * via Redis service and can be both custom or trending. 
 * @author ailiakop
 * @email ailiakop@iti.gr
 */
public class MediaSearcher {
	
	private static String REDIS_HOST = "redis.host";
	private static String SOLR_HOST = "solr.hostname";
	private static String SOLR_SERVICE = "solr.service";
	private static String DYSCO_COLLECTION = "dyscos.collection";
	private static String CHANNEL = "channel";
	
	public final Logger logger = Logger.getLogger(MediaSearcher.class);
	
	enum MediaSearcherState {
		OPEN, CLOSE
	}
	
	private MediaSearcherState state = MediaSearcherState.CLOSE;
	
	private StreamsManagerConfiguration config = null;
	
	private StoreManager storeManager;
	
	private StreamsMonitor monitor;

	private Jedis jedisClient;
	private Thread listener;
	
	private DyscoRequestHandler dyscoRequestHandler;
	private DyscoRequestReceiver dyscoRequestReceiver;
	
	private DyscoUpdateThread dyscoUpdateThread;
	
	// Handlers of Incoming Dyscos
	private TrendingSearchHandler trendingSearchHandler;
	private CustomSearchHandler customSearchHandler;
	
	private SolrQueryBuilder queryBuilder;
	
	private String redisHost;
	private String solrHost;
	private String solrService;
	private String dyscoCollection;
	
	private Map<String, Stream> streams = null;
	
	private Map<String, List<Query>> dyscosToQueries = new HashMap<String, List<Query>>();
	
	private Queue<Dysco> requests = new LinkedList<Dysco>();
	private Queue<String> requestsToDelete = new LinkedList<String>();
	private Queue<String> dyscosToUpdate = new LinkedList<String>();
	
	public MediaSearcher(StreamsManagerConfiguration config) throws StreamException {
		
		if (config == null) {
			throw new StreamException("Manager's configuration must be specified");
		}

		this.config = config;
		this.redisHost = config.getParameter(MediaSearcher.REDIS_HOST);
		this.solrHost = config.getParameter(MediaSearcher.SOLR_HOST);
		this.solrService = config.getParameter(MediaSearcher.SOLR_SERVICE);
		this.dyscoCollection = config.getParameter(MediaSearcher.DYSCO_COLLECTION);
		
		
		//Set up the Streams
		initStreams();
		
		//Set up the Storages
		storeManager = new StoreManager(config);
	}
	
	/**
	 * Opens Manager by starting the auxiliary modules and setting up
	 * the databases for reading/storing
	 * @throws StreamException
	 */
	public synchronized void open() throws StreamException {
		
		if (state == MediaSearcherState.OPEN) {
			return;
		}
		
		state = MediaSearcherState.OPEN;
		
		storeManager.start();	
		logger.info("Store Manager is ready to store.");
		
		Set<String> failedStreams = new HashSet<String>();
		for (String streamId : streams.keySet()) {
			try {
				logger.info("MediaSearcher - Start Stream : " + streamId);
				StreamConfiguration sconfig = config.getStreamConfig(streamId);
				Stream stream = streams.get(streamId);
				stream.setHandler(storeManager);
				stream.open(sconfig);
			}
			catch(Exception e) {
				logger.error("Stream " + streamId + " filed to open.");
				failedStreams.add(streamId);
			}
		}
		for (String streamId : failedStreams) {
			streams.remove(streamId);
		}
		logger.info(streams.size() + " Streams are now open");
		
		//If there are Streams to monitor start the StreamsMonitor
		if(streams != null && !streams.isEmpty()) {
			monitor = new StreamsMonitor(streams.size());
			monitor.addStreams(streams);
			logger.info("Streams added to monitor");
		}
		else {
			logger.error("Streams Monitor cannot be started");
		}
		
		//start handlers
		this.dyscoRequestHandler = new DyscoRequestHandler();
		this.dyscoRequestReceiver = new DyscoRequestReceiver();
		this.dyscoUpdateThread = new DyscoUpdateThread();
		this.trendingSearchHandler = new TrendingSearchHandler(this);
		this.customSearchHandler = new CustomSearchHandler(this);
		
		try {
			this.queryBuilder = new SolrQueryBuilder();
		} catch (Exception e) {
			logger.error(e);
		}
		
		dyscoRequestHandler.start();
		dyscoUpdateThread.start();
        trendingSearchHandler.start();
		customSearchHandler.start();
		
		logger.info("Connect to redis...");
    	JedisPoolConfig poolConfig = new JedisPoolConfig();
        JedisPool jedisPool = new JedisPool(poolConfig, redisHost, 6379, 0);
        jedisClient = jedisPool.getResource();
        
		this.listener = new Thread(new Runnable() {
            @Override
            public void run() {
                try {          
                    logger.info("Subscribe to redis...");
                    jedisClient.subscribe(dyscoRequestReceiver, config.getParameter(MediaSearcher.CHANNEL));  
                                  
                    logger.info("subscribe returned, closing down");
                    jedisClient.quit();
                } catch (Exception e) {
                	logger.error(e);
                }
            }
        });
		listener.start();
		
		state = MediaSearcherState.OPEN;
		Runtime.getRuntime().addShutdownHook(new Shutdown(this));
	}
	
	/**
	 * Closes Manager along with its auxiliary modules
	 * @throws StreamException
	 */
	public synchronized void close() throws StreamException {
		
		if (state == MediaSearcherState.CLOSE) {
			return;
		}
		
		try {
			for (Stream stream : streams.values()) {
				stream.close();
			}
			
			if(jedisClient != null) {
				jedisClient.close();
				logger.info("Jedis Connection is closed");
			}
			
			if(dyscoRequestHandler != null) {
				dyscoRequestHandler.close();
				logger.info("DyscoRequestHandler is closed.");
			}
			state = MediaSearcherState.CLOSE;
			logger.info("MediaSearcher closed.");
		}catch(Exception e) {
			throw new StreamException("Error during streams close", e);
		}
	}

	/**
	 * Searches in all social media defined in the configuration file
	 * for the list of feeds that is given as input and returns the retrieved items
	 * @param feeds
	 * @param streamsToSearch
	 * @return the list of the items retrieved
	 */
	public synchronized List<Item> search(List<Feed> feeds) {
		if(feeds != null && !feeds.isEmpty()) {
			try {
				monitor.retrieveFromAllStreams(feeds);	
				while(!monitor.areAllStreamsFinished()) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						logger.error(e);
					}
				}		
			} catch (Exception e) {
				logger.error(e);
			}
		}
		return monitor.getTotalRetrievedItems();
	}
	
	
	/**
	 * Initializes the streams apis that are going to be searched for 
	 * relevant content
	 * @throws StreamException
	 */
	private void initStreams() throws StreamException {
		streams = new HashMap<String, Stream>();
		
		Set<String> streamIds = config.getStreamIds();
		for(String streamId : streamIds) {
			try {
				StreamConfiguration sconfig = config.getStreamConfig(streamId);
				streams.put(streamId,(Stream)Class.forName(sconfig.getParameter(StreamConfiguration.CLASS_PATH)).newInstance());
			}
			catch(Exception e) {
				e.printStackTrace();
				logger.error("Error during streams initialization", e);
			}
		}
	}
	
	/**
	 * Class that implements the Redis Client. Receives the DySco request to be served
	 * as a message. The request might entail the creation of a DySco (NEW), the update of
	 * a DySco (UPDATE) or the deletion of a DySco (DELETE). The service finds the received
	 * DySco in Solr database by its id and passes on the request to the Search Handler modules.
	 * @author ailiakop
	 */
	public class DyscoRequestReceiver extends JedisPubSub {

		private SolrDyscoHandler solrDyscoHandler;
		
		public DyscoRequestReceiver() {
			this.solrDyscoHandler = SolrDyscoHandler.getInstance(solrHost+"/"+solrService+"/"+dyscoCollection);
		}
		
	    @Override
	    public void onMessage(String channel, String message) {
	    	
	    	logger.info("Received dysco request : " + message);
	    	Message dyscoMessage = Message.create(message);
	    	
	    	String dyscoId = dyscoMessage.getDyscoId();    	
	    	Dysco dysco = solrDyscoHandler.findDyscoLight(dyscoId);
    		
    		if(dysco == null) {
    			logger.error("Invalid dysco request");
    			return;
    		}
	    	
    		Action action = dyscoMessage.getAction();
	    	switch(action) {
		    	case NEW : 
		    		logger.info("New dysco with id : " + dyscoId + " created");
		    		requests.add(dysco);
		    		break;
		    	case UPDATE:
		    		logger.info("Dysco with id : " + dyscoId + " updated");
		    		//to be implemented
		    		break;
		    	case DELETE:
		    		if(requests.contains(dysco)) {
		    			requests.remove(dysco);
		    		}
		    		else {
		    			requestsToDelete.add(dyscoId);
		    		}
		    		logger.info("Dysco with id : " + dyscoId + " deleted");
		    		break;
	    	}
	    	
	    }
	 
	    @Override
	    public void onPMessage(String pattern, String channel, String message) {
	    	// Do Nothing
	    }
	 
	    @Override
	    public void onSubscribe(String channel, int subscribedChannels) {
	    	// Do Nothing
	    }
	 
	    @Override
	    public void onUnsubscribe(String channel, int subscribedChannels) {
	    	// Do Nothing
	    }
	 
	    @Override
	    public void onPUnsubscribe(String pattern, int subscribedChannels) {
	    	// Do Nothing
	    }
	 
	    @Override
	    public void onPSubscribe(String pattern, int subscribedChannels) {
	    	// Do Nothing
	    }
	}
	
	
	/**
	 * Class responsible for setting apart trending from custom DyScos and creating the appropriate
	 * feeds for searching them. Afterwards, it adds the DySco to the queue for further 
	 * processing from the suitable search handler. 
	 * @author ailiakop
	 *
	 */
	private class DyscoRequestHandler extends Thread {

		private boolean isAlive = true;
	
		public DyscoRequestHandler() {
			
		}
		
		public void run() {
			
			Dysco receivedDysco = null;
			while(isAlive) {
				receivedDysco = poll();
				if(receivedDysco == null) {
					try {
						//Thread.sleep(10000);
						this.wait(10000);
					} catch (InterruptedException e) {
						logger.error(e);
					}
					continue;
				}
				else {
					try {
						FeedsCreator feedsCreator = new FeedsCreator(DataInputType.DYSCO, receivedDysco);
						List<Feed> feeds = feedsCreator.getQuery();
					
						DyscoType dyscoType = receivedDysco.getDyscoType();
						
						if(dyscoType.equals(DyscoType.TRENDING)) {
							trendingSearchHandler.addTrendingDysco(receivedDysco, feeds);
						}
						else if(dyscoType.equals(DyscoType.CUSTOM)) {
							customSearchHandler.addCustomDysco(receivedDysco.getId(), feeds);
						}
						else {
							logger.error("Unsupported dysco type - Cannot be processed from MediaSearcher");
						}
					}
					catch(Exception e) {
						logger.error(e);
					}
				}
			}
		}
		
		/**
		 * Polls a  dysco request from the queue
		 * @return
		 */
		private Dysco poll() {
			synchronized (requests) {					
				Dysco request = requests.poll();
				return request;
			}
		}
		
		public void close() {
			isAlive = false;
			try {
				this.interrupt();
			}
			catch(Exception e) {
				logger.error("Failed to interrupt itself", e);
			}
		}
	}
	
	/**
	 * Class responsible for updating a DySco's solr queries after they have been computed from the
	 * query builder (query expansion module)
	 * @author ailiakop
	 *
	 */
	public class DyscoUpdateThread extends Thread {
		
		private SolrDyscoHandler solrdyscoHandler;
		private boolean isAlive = true;
		
		public DyscoUpdateThread() {
			this.solrdyscoHandler = SolrDyscoHandler.getInstance(solrHost+"/"+solrService+"/"+dyscoCollection);
		}
		
		public void run() {
			String dyscoToUpdate = null;
			
			while(isAlive) {
				dyscoToUpdate = poll();
				if(dyscoToUpdate == null) {
					try {
						//Thread.sleep(1000);
						this.wait(1000);
					} catch (InterruptedException e) {
						logger.error(e);
					}
					continue;
				}
				else {
					try {
						List<Query> solrQueries = dyscosToQueries.get(dyscoToUpdate);

						Dysco updatedDysco = solrdyscoHandler.findDyscoLight(dyscoToUpdate);
						updatedDysco.getSolrQueries().clear();
						updatedDysco.setSolrQueries(solrQueries);
						solrdyscoHandler.insertDysco(updatedDysco);
						dyscosToQueries.remove(dyscoToUpdate);
					
						logger.info("Dysco: " + dyscoToUpdate + " is updated");
					}
					catch(Exception e) {
						logger.error(e);
					}
				}
			}
		}
		
		/**
		 * Polls a DySco request from the queue to update
		 * @return
		 */
		private String poll() {
			synchronized (dyscosToUpdate) {					
				if (!dyscosToUpdate.isEmpty()) {
					return dyscosToUpdate.poll();
				}
				return null;
			}
		}
		
		public void close() {
			isAlive = false;
			try {
				
			}
			catch(Exception e) {
				logger.error("Failed to interrupt itself", e);
			}
		}
	}
	
	
	/**
	 * Class in case system is shutdown 
	 * Responsible to close all services that are running at the time 
	 * @author ailiakop
	 */
	
	private class Shutdown extends Thread {
		private MediaSearcher searcher = null;

		public Shutdown(MediaSearcher searcher) {
			this.searcher = searcher;
		}

		public void run() {
			System.out.println("Shutting down media searcher ...");
			if (searcher != null) {
				try {
					searcher.close();
				} catch (StreamException e) {
					e.printStackTrace();
				}
			}
			System.out.println("Done...");
		}
	}
	

	/**
	 * Class responsible for Custom DySco requests 
	 * Custom DyScos are searched periodically in the system in a two-day period of time. 
	 * They are deleted in case the user deletes them or the searching period has expired. 
	 * The frequency Custom DyScos are searched in the system has been set in 10 minutes.
	 * @author ailiakop
	 *
	 */
	public class CustomSearchHandler extends Thread {
		
		private BlockingQueue<String> customDyscoQueue = new LinkedBlockingQueue<String>();
		
		private Map<String, List<Feed>> inputFeedsPerDysco = Collections.synchronizedMap(new HashMap<String, List<Feed>>());
		private Map<String, Long> requestsLifetime = Collections.synchronizedMap(new HashMap<String, Long>());
		private Map<String, Long> requestsTimestamps = Collections.synchronizedMap(new HashMap<String, Long>());
		
		private MediaSearcher searcher;

		private boolean isAlive = true;
		
		private static final long frequency = 10 * 60 * 1000; 			//ten minutes
		private static final long periodOfTime = 2 * 24 * 3600 * 1000; 	//two days
		
		public CustomSearchHandler(MediaSearcher mediaSearcher) {
			this.searcher = mediaSearcher;
		}
		
		public void addCustomDysco(String dyscoId, List<Feed> inputFeeds) {
			logger.info("New incoming Custom DySco: " + dyscoId + " with " + inputFeeds.size() + " searchable feeds");
			try {
				customDyscoQueue.add(dyscoId);
				inputFeedsPerDysco.put(dyscoId, inputFeeds);
				
				long timestamp = System.currentTimeMillis();
				requestsLifetime.put(dyscoId, timestamp);
				requestsTimestamps.put(dyscoId, timestamp);
			}
			catch(Exception e) {
				logger.error(e);
				customDyscoQueue.remove(dyscoId);
				deleteCustomDysco(dyscoId);
			}
		}
		
		public void deleteCustomDysco(String dyscoId) {
			inputFeedsPerDysco.remove(dyscoId);
			requestsLifetime.remove(dyscoId);
			requestsTimestamps.remove(dyscoId);
		}
		
		public void run() {
			String dyscoId = null;
			while(isAlive) {
				try {
					updateCustomQueue();
				}
				catch(Exception e) {
					logger.error("Failed to update Custom Dyscos Queue.", e);
				}
				
				dyscoId = poll();
				if(dyscoId == null) {
					try {
						this.wait(1000);
					} catch (InterruptedException e) {
						logger.error(e);
					}
					continue;
				}
				else {
					logger.info("Media Searcher handling #" + dyscoId);
					try {
						List<Feed> feeds = inputFeedsPerDysco.get(dyscoId);
						List<Item> customDyscoItems = searcher.search(feeds);
						logger.info("Total Items retrieved for Custom DySco " + dyscoId + " : " + customDyscoItems.size());
						customDyscoItems.clear();
					}
					catch(Exception e) {
						logger.error(e);
					}
				}
			}
		}
		
		/**
		 * Polls a custom DySco request from the queue to search
		 * @return
		 */
		private String poll() {
			String request = null;
			try {
				request = customDyscoQueue.take();
			} catch (InterruptedException e) {

			}
			return request;
		}
		
		/**
		 * Stops Custom Search Handler
		 */
		public synchronized void close() {
			isAlive = false;
			try {
				this.interrupt();
			}
			catch(Exception e) {
				logger.error("Failed to interrupt itself", e);
			}
		}
	
		/**
		 * Updates the queue of custom DyScos and re-examines or deletes 
		 * them according to their time in the system
		 */
		private synchronized void updateCustomQueue() {
			
			List<String> requestsToRemove = new ArrayList<String>();
			long currentTime = System.currentTimeMillis();
			
			for(Map.Entry<String, Long> entry : requestsLifetime.entrySet()) {
				String key = entry.getKey();
				Long value = entry.getValue();
				if(currentTime - value > frequency) {
					
					logger.info("Custom DySco " +  key + "  frequency: " + frequency + " currentTime: " + currentTime + 
							" dysco's last search time: " + value);
					
					if(currentTime - requestsTimestamps.get(key)> periodOfTime) {
						logger.info("periodOfTime: " + periodOfTime + " currentTime: " + currentTime + " dysco's lifetime: " + requestsTimestamps.get(key));
						logger.info("Remove Custom DySco " + key + " from the queue - expired");
						requestsToRemove.add(key);
						continue;
					}
					
					logger.info("Add Custom DySco " + key + " again in the queue for searching");
					customDyscoQueue.add(key);
					requestsLifetime.put(key, System.currentTimeMillis());	
				}
			}
			
			if(!requestsToRemove.isEmpty() || !requestsToDelete.isEmpty()) {
				for(String requestToRemove : requestsToRemove) {
					deleteCustomDysco(requestToRemove);
				}
				for(String requestToDelete : requestsToDelete) {
					deleteCustomDysco(requestToDelete);
				}
				requestsToRemove.clear();	
				requestsToDelete.clear();
			}
		}
	}
	
	/**
	 * Class responsible for Trending DySco requests 
	 * It performs custom search to retrieve an item collection
	 * based on Dysco's primary queries and afterwards applies query
	 * expansion via the Query Builder module to extend the search
	 * and detect more relative content to the DySco. 
	 * 
	 * @author ailiakop
	 */
	public class TrendingSearchHandler extends Thread {
		
		private BlockingQueue<Dysco> trendingDyscoQueue = new LinkedBlockingDeque<Dysco>(100);
		private Map<String, List<Feed>> inputFeedsPerDysco = new HashMap<String, List<Feed>>();
		private List<Item> retrievedItems = new ArrayList<Item>();
		
		private MediaSearcher searcher;

		private boolean isAlive = true;
		
		private Date retrievalDate; 

		public TrendingSearchHandler(MediaSearcher mediaSearcher) {
			this.searcher = mediaSearcher;
		}
		
		public void addTrendingDysco(Dysco dysco, List<Feed> inputFeeds) {
			try {
				logger.info("New incoming Trending DySco: " + dysco.getId() + " with " + inputFeeds.size() + " searchable feeds");
				trendingDyscoQueue.put(dysco);
				synchronized (inputFeedsPerDysco) {	
					inputFeedsPerDysco.put(dysco.getId(), inputFeeds);
				}
			}
			catch(Exception e) {
				logger.error(e);
			}
		}
		
		public void run() {
			Dysco dysco = null;
			while(isAlive) {
				dysco = poll();
				if(dysco == null) {
					try {
						//Thread.sleep(5000);
						this.wait(5000);
					} catch (InterruptedException e) {
						logger.error(e);
					}
					continue;
				}
				else {
					try {
						searchForTrendingDysco(dysco);
					}
					catch(Exception e) {
						logger.error("Error during searching for trending dysco " + dysco.getId(), e);
					}
				}
			}
		}
		
		/**
		 * Polls a trending DySco request from the queue to search
		 * @return
		 */
		private Dysco poll() {
			Dysco request = trendingDyscoQueue.poll();
			return request;
		}
		
		/**
		 * Searches for a Trending DySco at two stages. At the first stage the algorithm 
		 * retrieves content from social media using DySco's primal queries, whereas 
		 * at the second stage it uses the retrieved content to expand the queries.
		 * The final queries, which are the result of merging the primal and the expanded
		 * queries are used to search further in social media for additional content. 
		 * @param dysco
		 */
		private synchronized void searchForTrendingDysco(Dysco dysco) {
			
			long start = System.currentTimeMillis();
			logger.info("Media Searcher handling #" + dysco.getId());
			
			//first search
			List<Feed> feeds = inputFeedsPerDysco.get(dysco.getId());
			retrievalDate = new Date(System.currentTimeMillis());
			inputFeedsPerDysco.remove(dysco.getId());
			retrievedItems = searcher.search(feeds);
			
			long t1 = System.currentTimeMillis();
			logger.info("Time for First Search for Trending DySco: " + dysco.getId() + " is " + (t1-start)/1000 + " sec.");
			logger.info("Items retrieved for Trending DySco : " + dysco.getId() + " : " + retrievedItems.size());
			
			long t2 = System.currentTimeMillis();
			
			//second search
			
			// Expand Queries
			List<Query> queries = queryBuilder.getExpandedSolrQueries(retrievedItems, dysco, 5);
			List<Query> expandedQueries = new ArrayList<Query>();
			for(Query q : queries) {
				if(q.getIsFromExpansion()) {
					expandedQueries.add(q);
				}
			}
			logger.info("Number of additional queries for Trending DySco: " + dysco.getId() + " is " + expandedQueries.size());
			
			long t3 = System.currentTimeMillis();
			logger.info("Time for computing queries for Trending DySco: " + dysco.getId() + " is " + (t3-t2)/1000 + " sec.");
			
			if(!expandedQueries.isEmpty()) {
				List<Feed> newFeeds = transformQueriesToKeywordsFeeds(expandedQueries, retrievalDate);
				List<Item> secondSearchItems = searcher.search(newFeeds);
				
				long t4 = System.currentTimeMillis();
				logger.info("Total Items retrieved for Trending DySco : " + dysco.getId() + " : " + secondSearchItems.size());
				logger.info("Time for Second Search for Trending DySco: " + dysco.getId() + " is " + (t4-t3)/1000 + " sec.");
			
				dyscosToQueries.put(dysco.getId(), queries);
				dyscosToUpdate.add(dysco.getId());
			}
			else {
				System.out.println("No queries to update");
			}
			long end = System.currentTimeMillis();
			
			logger.info("Total Time searching for Trending DySco: " + dysco.getId() + " is " + (end-start)/1000 + " sec.");
		}
		
		
		/**
		 * Stops TrendingSearchHandler
		 */
		public synchronized void close() {
			isAlive = false;
			try {
				this.interrupt();
			}
			catch(Exception e) {
				logger.error("Failed to interrupt itself", e);
			}
		}
		
		/**
		 * Transforms Query instances to KeywordsFeed instances that will be used 
		 * for searching social media
		 * @param queries
		 * @param dateToRetrieve
		 * @return the list of feeds
		 */
		private List<Feed> transformQueriesToKeywordsFeeds(List<Query> queries,Date dateToRetrieve) {	
			List<Feed> feeds = new ArrayList<Feed>();
			
			for(Query query : queries){
				UUID UUid = UUID.randomUUID(); 
				feeds.add(new KeywordsFeed(new Keyword(query.getName(),query.getScore()),dateToRetrieve,UUid.toString()));
			}
			
			return feeds;
		}
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		File configFile = null;
		if(args.length != 1 ) {
			configFile = new File("./conf/mediasearcher.conf.xml");
		}
		else {
			configFile = new File(args[0]);
		}
		
		try {
			StreamsManagerConfiguration config = StreamsManagerConfiguration.readFromFile(configFile);
			MediaSearcher mediaSearcher = new MediaSearcher(config);
			mediaSearcher.open();
			
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (StreamException e) {
			e.printStackTrace();
		}
	}

}
