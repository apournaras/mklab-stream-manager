package eu.socialsensor.sfc.streams.management;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.xml.sax.SAXException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import eu.socialsensor.framework.client.dao.DyscoRequestDAO;
import eu.socialsensor.framework.client.dao.ItemDAO;
import eu.socialsensor.framework.client.dao.impl.DyscoRequestDAOImpl;
import eu.socialsensor.framework.client.dao.impl.ItemDAOImpl;
import eu.socialsensor.framework.client.search.MediaSearcher;
import eu.socialsensor.framework.client.search.solr.SolrDyscoHandler;
import eu.socialsensor.framework.common.domain.DyscoRequest;
import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.common.domain.Feed.FeedType;
import eu.socialsensor.framework.common.domain.dysco.Dysco;
import eu.socialsensor.framework.common.domain.dysco.Entity;
import eu.socialsensor.framework.common.domain.feeds.KeywordsFeed;
import eu.socialsensor.framework.streams.StreamException;
import eu.socialsensor.sfc.streams.StorageConfiguration;
import eu.socialsensor.sfc.streams.StreamsManagerConfiguration;
import eu.socialsensor.sfc.streams.input.RSSProcessor;
import eu.socialsensor.sfc.streams.input.FeedsCreatorImpl.CustomFeedsCreator;
import eu.socialsensor.sfc.streams.input.FeedsCreatorImpl.DynamicFeedsCreator;


/**
 * @brief  Class for receiving dysco requests and extracting 
 * the representative keywords that will be used for further 
 * search with the wrappers of social networks
 * 
 * @author ailiakop
 * @email  ailiakop@iti.gr
 *
 */
public class DyscoManager {
	private static String GLOBAL_HOST = "mongodb.host";
	private static String CLIENT_HOST = "mongodb.client.host";
	private static String DATABASE = "mongodb.database";
	private static String DYSCO_REQUESTS = "mongodb.dyscos.collection";
	private static String RSS_TOPICS = "mongodb.rss.collection";
	
	Logger logger = Logger.getLogger(DyscoManager.class);
	
	enum DyscoManagerState {
		OPEN, CLOSE
	}

	private DyscoManagerState state = DyscoManagerState.CLOSE;
	private StreamsManagerConfiguration config = null;
	private DyscoRequestHandler dyscoRequestHandler;
	private Jedis subscriberJedis;
	
	private String host;
	private String privateHost;
	private String dbName;
	private String dyscoRequestsCollection;
	private String rssCollectionName;
	
	private DyscoRequestDAO dyscoRequestDAO;
	private ItemDAO itemDAO;
	
	private RSSProcessor rssProcessor;
	private RSSUpdator rssUpdator;
	private DyscoRequestFeedsCreator drf_creator;
	
	private Queue<String> requests = new LinkedList<String>();
	
	public DyscoManager(StreamsManagerConfiguration config) throws StreamException{
		if (config == null) {
			throw new StreamException("Manager's configuration must be specified");
		}
		
		this.config = config;
		
		StorageConfiguration storage_config = config.getStorageConfig("Mongodb");
		this.host = storage_config.getParameter(DyscoManager.GLOBAL_HOST);
		this.dbName = storage_config.getParameter(DyscoManager.DATABASE);
		this.privateHost = storage_config.getParameter(DyscoManager.CLIENT_HOST);
		this.dyscoRequestsCollection = storage_config.getParameter(DyscoManager.DYSCO_REQUESTS, "Dyscos");
		this.rssCollectionName = storage_config.getParameter(DyscoManager.RSS_TOPICS, "Topics");
		
        
        Runtime.getRuntime().addShutdownHook(new Shutdown(this));
	}
	
	/**
	 * Opens Manager by starting the auxiliary modules and setting up
	 * the database for reading/storing
	 * @throws StreamException
	 */
	public synchronized void open() throws StreamException {	
		if (state == DyscoManagerState.OPEN){
			return;
		}
		
		logger.info("Open MongoDB storage <host: " + host + ", database: " + dbName + 
				", dyscos' collection: " + dyscoRequestsCollection +">");
		
		this.dyscoRequestDAO = new DyscoRequestDAOImpl(host, dbName, dyscoRequestsCollection);
		
		rssProcessor = new RSSProcessor();
		
		rssUpdator = new RSSUpdator(this);
		rssUpdator.start();
		
		drf_creator = new DyscoRequestFeedsCreator();
		drf_creator.start();
		
		this.dyscoRequestHandler = new DyscoRequestHandler(this);
		JedisPoolConfig poolConfig = new JedisPoolConfig();
        JedisPool jedisPool = new JedisPool(poolConfig, privateHost, 6379, 0);
        this.subscriberJedis = jedisPool.getResource();
		
		System.out.println("Dysco Manager is up and running! Wait for dysco requests");
		
		new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                	logger.info("Try to subscribe to redis");
                    subscriberJedis.subscribe(dyscoRequestHandler, MediaSearcher.CHANNEL);
                   
                } catch (Exception e) {
                }
            }
        }).start();
		
		state = DyscoManagerState.OPEN;
	}
	
	/**
	 * Sets the RSSProcessor that is used for handling
	 * the rss topics for keywords' extraction
	 * @param itemDAO
	 */
	public void setRSSProcessor(ItemDAO itemDAO){
		rssProcessor.setRSSProcessor(itemDAO);
		rssProcessor.processRSSItems();
	}
	
	/**
	 * Resets the RSSProcessor because a new collection of
	 * rss topics will be used
	 */
	public void resetRSSProcessor(){
		rssProcessor.resetRSSProcessor();
	}
	
	/**
	 * Closes Manager along with its auxiliary modules
	 * @throws StreamException
	 */
	public synchronized void close() throws StreamException {	
		if (state == DyscoManagerState.CLOSE){
			return;
		}
		
		if(rssUpdator != null)
			rssUpdator.die();
		
		if(drf_creator!=null)
			drf_creator.die();
		
		state = DyscoManagerState.CLOSE;
	}
	
	/**
	 * Class for the constant update of rss topics in the system. 
	 * Updates the collection of the rss topics every one hour.
	 * @author ailiakop
	 *
	 */
	private class RSSUpdator extends Thread {
		private long oneHour = 3600000;
		private DyscoManager dyscoManager;
		private boolean isAlive = true;
		private int prev_day = 0 ;
		
		public RSSUpdator(DyscoManager dyscoManager){
			this.dyscoManager = dyscoManager;
			
			updateRSSItems();
		}
		
		public void run(){
			System.out.println("RSSUpdator has started running");
			
			while(isAlive){
				
				try {
					Thread.sleep(oneHour);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				updateRSSItems();
			}
		}
		
		/**
		 * Updates the DB that holds daily rss topics
		 * For every day of the week a new DB is created 
		 * new rss feeds to be stored.
		 */
		
		public void updateRSSItems(){
			
			DateTime currentDateTime = new DateTime();
			String currentDbName = "RSS_Topics_"+currentDateTime.getDayOfMonth()+"_"+currentDateTime.getMonthOfYear();
			
			if(prev_day!=(currentDateTime.getDayOfMonth())){
				dyscoManager.resetRSSProcessor();
			}
			
			System.out.println("Reading "+currentDbName+" DB");
			
			prev_day = currentDateTime.getDayOfMonth();
			itemDAO = new ItemDAOImpl(privateHost, currentDbName,rssCollectionName);
			
			logger.info("Open MongoDB storage for rssTopics <host: " + privateHost + ", database: " + currentDbName + 
					", rssTopics' collection: " + rssCollectionName +">");
		
			dyscoManager.setRSSProcessor(itemDAO);
			
		}
		
		public void die(){
			isAlive = false;
		}
	}
	
	/**
	 * Class in case of system's shutdown. 
	 * Responsible to close all services 
	 * that are running at the time being
	 * @author ailiakop
	 *
	 */
	private class Shutdown extends Thread {
		DyscoManager manager = null;

		public Shutdown(DyscoManager manager) {
			this.manager = manager;
		}

		public void run(){
			System.out.println("Shutting down Dysco Manager...");
			if (manager != null) {
				try {
					manager.close();
				} catch (StreamException e) {
					e.printStackTrace();
				}
			}
			System.out.println("Done...");
		}
	}
	
	/**
	 * Class for handling incoming dysco requests that are received with redis
	 * @author ailiakop
	 *
	 */
	private class DyscoRequestHandler extends JedisPubSub {
	
		private DyscoManager dyscoManager;
		private DyscoRequest request;
		
		public DyscoRequestHandler(DyscoManager dyscoManager){
			this.dyscoManager = dyscoManager;
		}
		
		/**
		 * Alerts the system that a new dysco request is received
		 * New dysco requests are added to a queue to be further
		 * processed by the DyscoRequestFeedsCreator thread.
		 * In case the dysco request already exists in mongo db,
		 * it is deleted from the system and not processed further.
		 */
	    @Override
	    public void onMessage(String channel, String message) {
	    	
	    	logger.info("Received dysco request : "+message);
	    	
	    	if(!dyscoRequestDAO.exists(message)){
	    		requests.add(message);
		    	
		    	logger.info("Dysco "+message+" stored!");
		    	
	    	}
	    	else{
	    		logger.info("Dysco already exists in Mongo!");
		    	
	    		DyscoRequest request = dyscoRequestDAO.getDyscoRequest(message);
	    		
				logger.info("Dysco :"+message+" to delete...");
				
				request.setIsSearched(true);
				dyscoRequestDAO.updateRequest(request);
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
	 * Creates the input feeds for the dysco if possible and
	 * seperates dyscos according to their type (custom/trending).
	 * For the creation of the input feeds, RSSProcessor and DynamicFeedsCreator
	 * classes are incorporated.
	 * Afterwards it stores the update to the selected DB. 
	 * @author ailiakop
	 *
	 */
	private class DyscoRequestFeedsCreator extends Thread{
		private boolean isAlive = true;
		private static final int KEYWORDS_LIMIT = 3;
		private SolrDyscoHandler dyscoHandler = SolrDyscoHandler.getInstance();
		
		public DyscoRequestFeedsCreator(){
			
		}
		
		public void run(){
			String requestMessage = null;
			while(isAlive){
				
				requestMessage = poll();
				if(requestMessage == null){
					continue;
				}
				else{
					processDyscoRequest(requestMessage);
				}
					
			}
		}
		
		/**
		 * Polls a trending dysco request from the queue
		 * @return
		 */
		private String poll(){
			synchronized (requests) {					
				if (!requests.isEmpty()) {
					String request = requests.remove();
					return request;
				}
				try {
					requests.wait(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				return null;
			}
		}
		
		/**
		 * Processes a dysco request according to its type. 
		 * @param dyscoRequestMessage
		 */
		private void processDyscoRequest(String dyscoRequestMessage){
			List<String> keywordsOfRequest = new ArrayList<String>();
    		List<KeywordsFeed> feedsOfRequest = new ArrayList<KeywordsFeed>();
    		
    		DyscoRequest request = new DyscoRequest(dyscoRequestMessage,new Date(System.currentTimeMillis()));
    		
			Dysco dysco = dyscoHandler.findDyscoLight(dyscoRequestMessage);
			
			findKeywordsAndFeeds(dysco,keywordsOfRequest,feedsOfRequest);
			
    		if(feedsOfRequest != null && !feedsOfRequest.isEmpty()){
				request.setKeywordsFeeds(feedsOfRequest);
				request.setKeywords(keywordsOfRequest);
				
			}
			else{
				logger.info("No Feeds could be created!");
		
				request.setIsSearched(true);
			}
			
    		if(dysco.getEvolution().equals("dynamic")){
				request.setDyscoType("custom");
				
			}
			else{
				request.setDyscoType("trending");
				
			}
				
    		
	    	dyscoRequestDAO.insertDyscoRequest(request);
		}
		
		 /**
	     * Detects keywords of dysco according to its type and creates input feeds
	     * with the above keywords.
	     * 
	     * @param dysco
	     * @param keywords
	     * @param feeds
	     */
	    private void findKeywordsAndFeeds(Dysco dysco,List<String> keywords, List<KeywordsFeed> feeds){
	    	if(dysco.getEvolution().equals("dynamic")){
	    		System.out.println("Custom dysco : "+dysco.getId());
	    		CustomFeedsCreator c_creator = new CustomFeedsCreator(dysco);
	    		keywords.addAll(c_creator.extractKeywords()); 
				
				if(keywords.size()>0){
					for(Feed feed : c_creator.createFeeds()){
						if(feed.getFeedtype().equals(FeedType.KEYWORDS)){
							KeywordsFeed keyFeed = (KeywordsFeed) feed;
							feeds.add(keyFeed);
						}
					}
				}
					
	    	}
	    	else{
	    		System.out.println("Trending dysco : "+dysco.getId());
	    		
	    		DynamicFeedsCreator dynamicCreator = new DynamicFeedsCreator(rssProcessor.getWordsToRSSItems());
	    		
	    		List<String> mostSimilarRSSTopics = dynamicCreator.extractSimilarRSSForDysco(dysco);
	    		
	    		//add most important entities first
	    		if(dynamicCreator.getMostImportantEntities() != null){
	    			for(Entity ent : dynamicCreator.getMostImportantEntities())
	    				keywords.add(ent.getName());
	    		}
	    		//if there is need for more keywords add those that were found to be relevant from similar rss topics
	    		if(keywords.size()<KEYWORDS_LIMIT){
	    			
		    		List<String> processorKeywords = rssProcessor.getTopKeywordsFromSimilarRSS(mostSimilarRSSTopics, dysco);
		    		
		    		Set<String> keywordsToAdd = new HashSet<String>();
		    		
		    		//remove possible duplicates
					for(String p_key : processorKeywords){
						boolean exists = false;
						for(String key : keywords){
						
							if(key.toLowerCase().equals(p_key.toLowerCase()) || key.toLowerCase().contains(p_key.toLowerCase())){
									exists = true;
									break;
							}
						}
						if(!exists){
						
							keywordsToAdd.add(p_key);
						}
					}
		
					for(String keyToAdd : keywordsToAdd){
						boolean exists = false;
						for(String keyToAdd_ : keywordsToAdd){
							
							if(!keyToAdd.equals(keyToAdd_)){
								if(keyToAdd_.contains(keyToAdd)){
									exists = true;
								}
							}
						}
						
						if(!exists)
							keywords.add(keyToAdd);
					}
					
	    		}
	    		
					
				dynamicCreator.setTopKeywords(keywords);
				
				//create feeds with the extracted keywords
				List<Feed> inputFeeds = dynamicCreator.createFeeds();
				for(Feed feed : inputFeeds){
					if(feed.getFeedtype().equals(FeedType.KEYWORDS)){
						KeywordsFeed keyFeed = (KeywordsFeed) feed;
						feeds.add(keyFeed);
					}
				}
				
			
	    	}
	    }
	    
	    /**
		 * Stops TrendingSearchHandler
		 */
		public synchronized void die(){
			isAlive = false;
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			
			File configFile;
			
			if(args.length != 1 ) {
				configFile = new File("./conf/dysco_manager_updated.conf.xml");
			}
			else {
				configFile = new File(args[0]);
	
			}
			
			StreamsManagerConfiguration config = StreamsManagerConfiguration.readFromFile(configFile);			
	        
			DyscoManager dyscoManager = new DyscoManager(config);
			dyscoManager.open();
			
			
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
