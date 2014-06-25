package eu.socialsensor.sfc.streams.management;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

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
	
	public final Logger logger = Logger.getLogger(StreamsManager.class);
	
	enum MediaSearcherState {
		OPEN, CLOSE
	}
	private MediaSearcherState state = MediaSearcherState.CLOSE;
	
	private StreamsManagerConfiguration config = null;
	
	private StoreManager storeManager;
	
	private StreamsMonitor monitor;

	private Jedis subscriberJedis;
	
	private DyscoRequestHandler dyscoRequestHandler;
	private DyscoRequestReceiver dyscoRequestReceiver;
	private DyscoUpdateAgent dyscoUpdateAgent;
	private TrendingSearchHandler trendingSearchHandler;
	private CustomSearchHandler customSearchHandler;
	private SolrQueryBuilder queryBuilder;
	
	private String redisHost;
	private String solrHost;
	private String solrService;
	private String dyscoCollection;
	
	private boolean keyHold = false;
	
	private Map<String, Stream> streams = null;
	
	private Map<String,List<Query>> dyscosToQueries = new HashMap<String,List<Query>>();
	
	private Queue<Dysco> requests = new LinkedList<Dysco>();
	private Queue<String> dyscosToUpdate = new LinkedList<String>();
	
	public MediaSearcher(StreamsManagerConfiguration config) throws StreamException{
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
		
		for (String streamId : streams.keySet()) {
			logger.info("MediaSearcher - Start Stream : "+streamId);
			StreamConfiguration sconfig = config.getStreamConfig(streamId);
			Stream stream = streams.get(streamId);
			stream.setHandler(storeManager);
			stream.open(sconfig);
		}
		
		logger.info("Streams are now open");
		
		//If there are Streams to monitor start the StreamsMonitor
		if(streams != null && !streams.isEmpty()){
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
		this.dyscoUpdateAgent = new DyscoUpdateAgent();
		this.trendingSearchHandler = new TrendingSearchHandler(this);
		this.customSearchHandler = new CustomSearchHandler(this);
		
		try {
			this.queryBuilder = new SolrQueryBuilder();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		dyscoRequestHandler.start();
		dyscoUpdateAgent.start();
        trendingSearchHandler.start();
		customSearchHandler.start();
		
		JedisPoolConfig poolConfig = new JedisPoolConfig();
        JedisPool jedisPool = new JedisPool(poolConfig, redisHost, 6379, 0);
        this.subscriberJedis = jedisPool.getResource();
     
		new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                	logger.info("Subscribe to redis...");
                
                    subscriberJedis.subscribe(dyscoRequestReceiver,config.getParameter(MediaSearcher.CHANNEL));
                   
                } catch (Exception e) {
                }
            }
        }).start();
		
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
		
		try{
			for (Stream stream : streams.values()) {
				stream.close();
			}
			
			if(dyscoRequestReceiver != null){
				dyscoRequestReceiver.close();
				logger.info("DyscoRequestReceiver is closed");
			}
			
			if(dyscoRequestHandler != null){
				dyscoRequestHandler.close();
				logger.info("DyscoRequestHandler is closed.");
			}
			state = MediaSearcherState.CLOSE;
			logger.info("MediaSearcher closed.");
			System.out.println("MediaSearcher is closed.");
		}catch(Exception e) {
			throw new StreamException("Error during streams close",e);
		}
	}

	/**
	 * Searches in all social media defined in the configuration file
	 * for the list of feeds that is given as input and returns the retrieved items
	 * @param feeds
	 * @param streamsToSearch
	 * @return the list of the items retrieved
	 */
	public synchronized List<Item> search(List<Feed> feeds){
	
		if(feeds != null && !feeds.isEmpty()){
			
			try {
				monitor.retrieveFromAllStreams(feeds);
				while(!monitor.areAllStreamsFinished()){
					
				}
				
			} catch (Exception e) {
				
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
		streams = new HashMap<String,Stream>();
		try{
			for (String streamId : config.getStreamIds()){
				StreamConfiguration sconfig = config.getStreamConfig(streamId);
				streams.put(streamId,(Stream)Class.forName(sconfig.getParameter(StreamConfiguration.CLASS_PATH)).newInstance());
			}
		}catch(Exception e) {
			e.printStackTrace();
			throw new StreamException("Error during streams initialization",e);
		}
	}
	
	/**
	 * Class that implements the Redis Client. Receives the DySco request to be served
	 * as a message. The request might entail the creation of a DySco (NEW), the update of
	 * a DySco (UPDATE) or the deletion of a DySco (DELETE). The service finds the received
	 * DySco in Solr database by its id and passes on the request to the Search Handler modules.
	 * @author ailiakop
	 *
	 */
	public class DyscoRequestReceiver extends JedisPubSub{

		private SolrDyscoHandler solrdyscoHandler;
		
		public DyscoRequestReceiver(){
			
			this.solrdyscoHandler = SolrDyscoHandler.getInstance(solrHost+"/"+solrService+"/"+dyscoCollection);
		}
		
	    @Override
	    public void onMessage(String channel, String message) {
	    	
	    	logger.info("Received dysco request : "+message);
	    	Message dyscoMessage = Message.create(message);
	    	
	    	String dyscoId = dyscoMessage.getDyscoId();
	    	Action action = dyscoMessage.getAction();
	    	
	    	switch(action){
		    	case NEW : 
		    		System.out.println("New dysco with id : "+dyscoId+" created");
		    		Dysco dysco = solrdyscoHandler.findDyscoLight(dyscoId);
		    		
		    		if(dysco == null){
		    			logger.error("Invalid dysco request");
		    			return;
		    		}
		    		
		    		requests.add(dysco);
		    		break;
		    	case UPDATE:
		    		//to be implemented
		    		logger.info("Dysco with id : "+dyscoId+" updated");
		    		break;
		    	case DELETE:
		    		//to be implemented
		    		logger.info("Dysco with id : "+dyscoId+" deleted");
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
	    
	    public void close(){
	    	logger.info("Closing redis...");
	    	subscriberJedis.quit();
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
		
		private FeedsCreator feedsCreator;
		
		private List<Feed> feeds;
		
		public DyscoRequestHandler(){
			
		}
		
		public void run(){
			Dysco receivedDysco = null;
			while(isAlive){
				receivedDysco = poll();
				if(receivedDysco == null){
					continue;
				}
				else{
					
					feedsCreator = new FeedsCreator(DataInputType.DYSCO,receivedDysco);
					feeds = feedsCreator.getQuery();
					
					if(receivedDysco.getDyscoType().equals(DyscoType.TRENDING)){
						trendingSearchHandler.addTrendingDysco(receivedDysco, feeds);
					}
					else if(receivedDysco.getDyscoType().equals(DyscoType.CUSTOM)){
						customSearchHandler.addCustomDysco(receivedDysco.getId(), feeds);
					}
					else{
						logger.error("Unsupported dysco - Cannot be processed from MediaSearcher");
					}
				}
			}
		}
		
		/**
		 * Polls a  dysco request from the queue
		 * @return
		 */
		private Dysco poll(){
			synchronized (requests) {					
				if (!requests.isEmpty()) {
					System.out.println("DyScos remaining to be served: "+requests.size());
					Dysco request = requests.poll();
					try {
						requests.wait(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					return request;
				}
				
				
				return null;
			}
		}
		
		public void close(){
			isAlive = false;
		}
	}
	
	/**
	 * Class responsible for updating a DySco's solr queries after they have been computed from the
	 * query builder (query expansion module)
	 * @author ailiakop
	 *
	 */
	public class DyscoUpdateAgent extends Thread{
		private SolrDyscoHandler solrdyscoHandler;
		private boolean isAlive = true;
		
		public DyscoUpdateAgent(){
			
			this.solrdyscoHandler = SolrDyscoHandler.getInstance(solrHost+"/"+solrService+"/"+dyscoCollection);
		}
		
		public void run(){
			String dyscoToUpdate = null;
			
			while(isAlive){
				dyscoToUpdate = poll();
				if(dyscoToUpdate == null){
					continue;
				}
				else{
					List<Query> solrQueries = dyscosToQueries.get(dyscoToUpdate);

					Dysco updatedDysco = solrdyscoHandler.findDyscoLight(dyscoToUpdate);
					updatedDysco.getSolrQueries().clear();
					updatedDysco.setSolrQueries(solrQueries);
					solrdyscoHandler.insertDysco(updatedDysco);
					dyscosToQueries.remove(dyscoToUpdate);
					
					logger.info("Dysco : "+dyscoToUpdate+" is updated");
				}
			}
		}
		
		/**
		 * Polls a DySco request from the queue to update
		 * @return
		 */
		private String poll(){
			synchronized (dyscosToUpdate) {					
				if (!dyscosToUpdate.isEmpty()) {
				
					return dyscosToUpdate.poll();
				}
				
				return null;
			}
		}
		
		public void close(){
			isAlive = false;
		}
	}
	
	
	/**
	 * Class in case system is shutdown 
	 * Responsible to close all services 
	 * that are running at the time 
	 * @author ailiakop
	 *
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
		private Queue<String> customDyscoQueue = new LinkedList<String>();
		
		private Map<String,List<Feed>> inputFeedsPerDysco = new HashMap<String,List<Feed>>();
		private Map<String,Long> requestsLifetime = new HashMap<String,Long>();
		private Map<String,Long> requestsTimestamps = new HashMap<String,Long>();
		
		private MediaSearcher searcher;

		private boolean isAlive = true;
		
		private static final long frequency = 2 * 300000; //ten minutes
		private static final long periodOfTime = 48 * 3600000; //two days
		
		public CustomSearchHandler(MediaSearcher mediaSearcher){
			this.searcher = mediaSearcher;
			
		}
		
		public void addCustomDysco(String dyscoId,List<Feed> inputFeeds){
			System.out.println("New incoming Custom DySco : "+dyscoId+" with "+inputFeeds.size()+" searchable feeds");
			customDyscoQueue.add(dyscoId);
			inputFeedsPerDysco.put(dyscoId, inputFeeds);
			requestsLifetime.put(dyscoId, System.currentTimeMillis());
			requestsTimestamps.put(dyscoId, System.currentTimeMillis());
		}
		
		public void deleteCustomDysco(String dyscoId){
			inputFeedsPerDysco.remove(dyscoId);
			requestsLifetime.remove(dyscoId);
			requestsTimestamps.remove(dyscoId);
		}
		
		public void run(){
			String dyscoId = null;
			while(isAlive){
				updateCustomQueue();
				dyscoId = poll();
				if(dyscoId == null){
					continue;
				}
				else{
					keyHold = true;
					System.out.println("Media Searcher handling #"+dyscoId);
					List<Feed> feeds = inputFeedsPerDysco.get(dyscoId);
					inputFeedsPerDysco.remove(dyscoId);
					List<Item> customItems = searcher.search(feeds);
					System.out.println("Total Items retrieved for Custom DySco : "+dyscoId+" : "+customItems.size());
					keyHold = false;
				}
				
			}
		}
		/**
		 * Polls a custom DySco request from the queue to search
		 * @return
		 */
		private String poll(){
			synchronized (customDyscoQueue) {					
				if (!customDyscoQueue.isEmpty() && !keyHold) {
					String request = customDyscoQueue.poll();
					return request;
				}
				
				return null;
			}
		}
		/**
		 * Stops Custom Search Handler
		 */
		public synchronized void close(){
			isAlive = false;
		}
	
		/**
		 * Updates the queue of custom DyScos and re-examines or deletes 
		 * them according to their time in the system
		 */
		private synchronized void updateCustomQueue(){
			
			List<String> requestsToRemove = new ArrayList<String>();
			long currentTime = System.currentTimeMillis();
			
			for(Map.Entry<String, Long> entry : requestsLifetime.entrySet()){
				
				if(currentTime - entry.getValue() > frequency){
					if(currentTime - requestsTimestamps.get(entry.getKey())> periodOfTime){
						
						requestsToRemove.add(entry.getKey());
						continue;
					}
					entry.setValue(currentTime);
					String requestToSearch = entry.getKey();
					System.out.println("Add Custom DySco "+requestToSearch+" again in the queue for searching");
					customDyscoQueue.add(requestToSearch);
					requestsLifetime.put(entry.getKey(), System.currentTimeMillis());
					
						
				}
				
			}
			
			if(!requestsToRemove.isEmpty()){
				for(String requestToRemove : requestsToRemove){
					deleteCustomDysco(requestToRemove);
				}
				requestsToRemove.clear();	
			}
			
		}
		
		
	}
	
	/**
	 * Class responsible for Trending DySco requests 
	 * It performs custom search to retrieve an item collection
	 * based on Dysco's primal queries and afterwards applies query
	 * expansion via the Query Builder module to extend the search
	 * and detect more relative content to the DySco. 
	 * @author ailiakop
	 *
	 */
	public class TrendingSearchHandler extends Thread {
		
		private BlockingQueue<Dysco> trendingDyscoQueue = new LinkedBlockingDeque<Dysco>(100);
		
		private Map<String,List<Feed>> inputFeedsPerDysco = new HashMap<String,List<Feed>>();
		
		private List<Item> retrievedItems = new ArrayList<Item>();
		
		private MediaSearcher searcher;

		private boolean isAlive = true;
		
		
		private Date retrievalDate; 

		public TrendingSearchHandler(MediaSearcher mediaSearcher){
			this.searcher = mediaSearcher;
		}
		
		public void addTrendingDysco(Dysco dysco,List<Feed> inputFeeds){
			logger.info("New incoming Trending DySco : "+dysco.getId()+" with "+inputFeeds.size()+" searchable feeds");
			System.out.println("New incoming Trending DySco : "+dysco.getId()+" with "+inputFeeds.size()+" searchable feeds");
			synchronized (trendingDyscoQueue) {	
				trendingDyscoQueue.add(dysco);
			}
			inputFeedsPerDysco.put(dysco.getId(), inputFeeds);
		}
		
		public void run(){
			Dysco dysco = null;
			while(isAlive){
				
				dysco = poll();
				if(dysco == null){
					continue;
				}
				else{
					keyHold = true;
					searchForTrendingDysco(dysco);
					keyHold = false;
				}
					
			}
		}
		/**
		 * Polls a trending DySco request from the queue to search
		 * @return
		 */
		private Dysco poll(){
			synchronized (trendingDyscoQueue) {					
				if (!trendingDyscoQueue.isEmpty() && !keyHold) {
					Dysco request = trendingDyscoQueue.poll();
					
					return request;
				}
				
				return null;
			}
		}
		
		/**
		 * Searches for a Trending DySco at two stages. At the first stage the algorithm 
		 * retrieves content from social media using DySco's primal queries, whereas 
		 * at the second stage it uses the retrieved content to expand the queries.
		 * The final queries, which are the result of merging the primal and the expanded
		 * queries are used to search further in social media for additional content. 
		 * @param dysco
		 */
		private synchronized void searchForTrendingDysco(Dysco dysco){
			long start = System.currentTimeMillis();
			logger.info("Media Searcher handling #"+dysco.getId());
			//first search
			List<Feed> feeds = inputFeedsPerDysco.get(dysco.getId());
			retrievalDate = new Date(System.currentTimeMillis());
			inputFeedsPerDysco.remove(dysco.getId());
			retrievedItems = searcher.search(feeds);
			
			long t1 = System.currentTimeMillis();
			
			System.out.println("Time for First Search for Trending DySco: "+dysco.getId()+" is "+(t1-start)/1000+" sec ");
			
			long t2 = System.currentTimeMillis();
			
			//second search
			List<Query> queries = queryBuilder.getExpandedSolrQueries(retrievedItems,dysco,5);
			
			List<Query> expandedQueries = new ArrayList<Query>();
			
			for(Query q : queries)
				if(q.getIsFromExpansion())
					expandedQueries.add(q);
			
			System.out.println("Number of additional queries for Trending DySco: "+dysco.getId()+" is "+expandedQueries.size());
			
			long t3 = System.currentTimeMillis();
			
			System.out.println("Time for computing queries for Trending DySco:+ "+dysco.getId()+" is "+(t3-t2)/1000+" sec ");
			
			if(!expandedQueries.isEmpty()){
				List<Feed> newFeeds = transformQueriesToKeywordsFeeds(expandedQueries,retrievalDate);
				
				List<Item> secondSearchItems = searcher.search(newFeeds);
				
				long t4 = System.currentTimeMillis();
				System.out.println("Total Items retrieved for Trending DySco : "+dysco.getId()+" : "+secondSearchItems.size());
				System.out.println("Time for Second Search for Trending DySco:+ "+dysco.getId()+" is "+(t4-t3)/1000+" sec ");
			
				dyscosToQueries.put(dysco.getId(), queries);
				dyscosToUpdate.add(dysco.getId());
			}
			else{
				System.out.println("No queries to update");
			}
			
			long end = System.currentTimeMillis();
			
			System.out.println(new Date(System.currentTimeMillis())+" - Total Time searching for Trending DySco:+ "+dysco.getId()+" is "+(end-start)/1000+" sec ");
		}
		
		/**
		 * Stops TrendingSearchHandler
		 */
		public synchronized void close(){
			isAlive = false;
		}
		
		/**
		 * Transforms Query instances to KeywordsFeed instances that will be used 
		 * for searching social media
		 * @param queries
		 * @param dateToRetrieve
		 * @return the list of feeds
		 */
		private List<Feed> transformQueriesToKeywordsFeeds(List<Query> queries,Date dateToRetrieve)
		{	
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
		File configFile;
		
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (StreamException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
