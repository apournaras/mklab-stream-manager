package eu.socialsensor.sfc.streams.management;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
import eu.socialsensor.sfc.streams.input.FeedsCreatorImpl.RSSTopicFeedsCreator;

public class DyscoManagerUpdated {
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
	
	//Store that way temporarily
	private String host;
	private String privateHost;
	private String dbName;
	private String dyscoRequestsCollection;
	private String rssCollectionName;
	
	private DyscoRequestDAO dyscoRequestDAO;
	private ItemDAO itemDAO;
	
	private RSSProcessor rssProcessor;
	private RSSUpdator rssUpdator;
	
	public DyscoManagerUpdated(StreamsManagerConfiguration config) throws StreamException{
		if (config == null) {
			throw new StreamException("Manager's configuration must be specified");
		}
		
		this.config = config;
		
		StorageConfiguration storage_config = config.getStorageConfig("Mongodb");
		this.host = storage_config.getParameter(DyscoManagerUpdated.GLOBAL_HOST);
		this.dbName = storage_config.getParameter(DyscoManagerUpdated.DATABASE);
		this.privateHost = storage_config.getParameter(DyscoManagerUpdated.CLIENT_HOST);
		this.dyscoRequestsCollection = storage_config.getParameter(DyscoManagerUpdated.DYSCO_REQUESTS, "Dyscos");
		this.rssCollectionName = storage_config.getParameter(DyscoManagerUpdated.RSS_TOPICS, "Topics");
		
        
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
	
	
	public void setRSSProcessor(ItemDAO itemDAO){
		rssProcessor.setRSSProcessor(itemDAO);
		rssProcessor.processRSSItems();
	}
	
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
		
		state = DyscoManagerState.CLOSE;
	}
	
	/**
	 * Class for the constant update of rss feeds in the system. 
	 * Updates the collection of the rss feeds every one hour.
	 * @author ailiakop
	 *
	 */
	private class RSSUpdator extends Thread {
		private long oneHour = 3600000;
		private DyscoManagerUpdated dyscoManager;
		private boolean isAlive = true;
		private int prev_day = 0 ;
		
		public RSSUpdator(DyscoManagerUpdated dyscoManager){
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
		 * Updates the DB that holds daily rss feeds
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
	 * Class in case system is shutdown 
	 * Responsible to close all services 
	 * that are running at the time being
	 * @author ailiakop
	 *
	 */
	private class Shutdown extends Thread {
		DyscoManagerUpdated manager = null;

		public Shutdown(DyscoManagerUpdated manager) {
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
	
		private SolrDyscoHandler dyscoHandler = SolrDyscoHandler.getInstance();
		private DyscoManagerUpdated dyscoManager;
		private DyscoRequest request;
		
		private int keywordsLimit = 3;
		
		public DyscoRequestHandler(DyscoManagerUpdated dyscoManager){
			this.dyscoManager = dyscoManager;
		}
		
		/**
		 * Alerts the system that a new dysco request is received
		 * Creates the input feeds for the dysco if possible and
		 * seperates dyscos according to their type (custom/trending).
		 * Afterwards it stores the update to the selected DB. 
		 */
	    @Override
	    public void onMessage(String channel, String message) {
	    	
	    	logger.info("Received dysco request : "+message);
	    	
	    	if(!dyscoRequestDAO.exists(message)){
	    		List<String> keywordsOfRequest = new ArrayList<String>();
	    		List<KeywordsFeed> feedsOfRequest = new ArrayList<KeywordsFeed>();
	    		
	    		request = new DyscoRequest(message,new Date(System.currentTimeMillis()));
	    		
	    		Dysco dysco = dyscoHandler.findDyscoLight(message);
	    		
	    		findKeywordsAndFeeds(dysco,keywordsOfRequest,feedsOfRequest);
	    		
	    		if(feedsOfRequest != null && !feedsOfRequest.isEmpty()){
					request.setKeywordsFeeds(feedsOfRequest);
					request.setKeywords(keywordsOfRequest);
					
				}
				else{
					logger.info("No Feeds could be created!");
					keywordsOfRequest.clear();
					for(Entity ent : dysco.getEntities()){
						if(ent.getType().equals(Entity.Type.PERSON)){
							keywordsOfRequest.add(ent.getName());
							if(keywordsOfRequest.size() >= keywordsLimit)
								break;
						}	
					}
					if(keywordsOfRequest.size() < keywordsLimit)	
						for(Entity ent : dysco.getEntities())
							if(ent.getType().equals(Entity.Type.LOCATION) || ent.getType().equals(Entity.Type.ORGANIZATION)){
								keywordsOfRequest.add(ent.getName());
								if(keywordsOfRequest.size() >= keywordsLimit)
									break;
							}
					
					request.setKeywords(keywordsOfRequest);
					request.setIsSearched(true);
				}
				
	    		if(dysco.getEvolution().equals("dynamic")){
					request.setDyscoType("custom");
					
				}
				else{
					request.setDyscoType("trending");
					
				}
					
	    		
		    	dyscoRequestDAO.insertDyscoRequest(request);
		    	
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
	    
	    /**
	     * Detects keywords of dysco according to its type and creates input feeds
	     * with the above keywords.
	     * 
	     * @param dysco
	     * @param keywords
	     * @param feeds
	     */
	    public void findKeywordsAndFeeds(Dysco dysco,List<String> keywords, List<KeywordsFeed> feeds){
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
	    		RSSTopicFeedsCreator rt_creator = new RSSTopicFeedsCreator(rssProcessor.getWordsToRSSItems());
	    		
	    		List<String> mostSimilarRSSTopics = rt_creator.extractSimilarRSSForDysco(dysco);
	    		
				keywords.addAll(rssProcessor.getTopKeywordsFromSimilarRSS(mostSimilarRSSTopics, dysco)); 
				
				if(keywords.size()>0){
					List<Feed> inputFeeds = rt_creator.createFeeds();
					for(Feed feed : inputFeeds){
						if(feed.getFeedtype().equals(FeedType.KEYWORDS)){
							KeywordsFeed keyFeed = (KeywordsFeed) feed;
							feeds.add(keyFeed);
						}
					}
				}
			
	    	}
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
	        
			DyscoManagerUpdated dyscoManager = new DyscoManagerUpdated(config);
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
