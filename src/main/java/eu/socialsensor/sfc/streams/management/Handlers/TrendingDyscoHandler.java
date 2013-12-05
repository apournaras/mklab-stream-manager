package eu.socialsensor.sfc.streams.management.Handlers;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;


import eu.socialsensor.framework.client.dao.DyscoRequestDAO;
import eu.socialsensor.framework.client.dao.impl.DyscoRequestDAOImpl;
import eu.socialsensor.framework.common.domain.DyscoRequest;
import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.common.domain.feeds.KeywordsFeed;
import eu.socialsensor.framework.streams.Stream;
import eu.socialsensor.framework.streams.StreamConfiguration;
import eu.socialsensor.framework.streams.StreamException;
import eu.socialsensor.sfc.streams.StorageConfiguration;
import eu.socialsensor.sfc.streams.StreamsManagerConfiguration;
import eu.socialsensor.sfc.streams.management.StoreManager;
import eu.socialsensor.sfc.streams.monitors.StreamsMonitor;


/**
 * Class for handling trending dysco requests
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class TrendingDyscoHandler{
	private static String HOST = "mongodb.host";
	private static String DATABASE = "mongodb.database";
	private static String DYSCO_REQUESTS = "mongodb.dyscos.collection";
	
	Logger logger = Logger.getLogger(TrendingDyscoHandler.class);
	
	enum TrendingDyscoHandlerState {
		OPEN, CLOSE
	}
	
	private TrendingDyscoHandlerState state = TrendingDyscoHandlerState.CLOSE;
	
	private Map<String, Stream> streams = null;
	private StreamsManagerConfiguration config = null;
	
	private DyscoRequestDAO dyscoRequestDAO;
	private String dyscoRequestsCollectionName;

	private StoreManager storeManager;
	private TrendingSearchHandler trendingSearchHandler;
	
	private String host;
	private String dyscoDBName;
	
	public TrendingDyscoHandler(StreamsManagerConfiguration config) 
			throws StreamException {
		
		if (config == null) {
			throw new StreamException("Manager's configuration must be specified");
		}
		
		this.config = config;
		
		StorageConfiguration storage_config = config.getStorageConfig("Mongodb");
		this.host = storage_config.getParameter(TrendingDyscoHandler.HOST);
		this.dyscoDBName = storage_config.getParameter(TrendingDyscoHandler.DATABASE);
		this.dyscoRequestsCollectionName = storage_config.getParameter(TrendingDyscoHandler.DYSCO_REQUESTS, "Dyscos");
		
		logger.info("Open MongoDB storage <host: " + host + ", database: " + dyscoDBName + 
				", items collection: " + dyscoRequestsCollectionName +">");
		
		this.dyscoRequestDAO = new DyscoRequestDAOImpl(host, dyscoDBName, dyscoRequestsCollectionName);
		
		initStreams();
		
		trendingSearchHandler = new TrendingSearchHandler(); 
		
        Runtime.getRuntime().addShutdownHook(new Shutdown(this));
       
	}
	/**
	 * Opens Manager by starting the auxiliary modules and setting up
	 * the database for reading/storing
	 * @throws StreamException
	 */
	public void open() throws StreamException {	
		
		if (state == TrendingDyscoHandlerState.OPEN){
			return;
		}
		
		storeManager = new StoreManager(config);
		storeManager.start();
		
		for (String streamId : config.getStreamIds()) {
			StreamConfiguration sconfig = config.getStreamConfig(streamId);
			Stream stream = streams.get(streamId);
			stream.setHandler(storeManager);
			stream.open(sconfig);
			
			if(!streamId.equals("Twitter"))
				if(!stream.setMonitor()){
					System.err.println("Feeds' monitor could not be set for stream : "+streamId);
					return;
				}
		}
		
		state = TrendingDyscoHandlerState.OPEN;
		
		trendingSearchHandler.start();
		
	}
	
	/**
	 * Constantly fetches new trending dysco requests and
	 * adds them to the queue to be served. 
	 */
	public void run(){
		while(state == TrendingDyscoHandlerState.OPEN){
			trendingSearchHandler.fetch();
		}
		
		logger.info("Trending Search Handler has stoped fetching");
	}
	/**
	 * Closes Manager along with its auxiliary modules
	 * @throws StreamException
	 */
	public void close() throws StreamException {
		if (state == TrendingDyscoHandlerState.CLOSE){
			return;
		}
		
		try{
			for (Stream stream : streams.values()){
				stream.close();
			}
			if (storeManager != null) {
				storeManager.stop();
			}
			
			if(trendingSearchHandler != null)
				trendingSearchHandler.die();
			
			state = TrendingDyscoHandlerState.CLOSE;
		}catch(Exception e){
			throw new StreamException("Error during streams close", e);
		}
	}
	/**
	 * Initializes the streams that correspond to the wrappers 
	 * that are used for multimedia retrieval
	 * @throws StreamException
	 */
	private void initStreams() throws StreamException {
		streams = new HashMap<String,Stream>();
		
		try{
			for (String streamId : config.getStreamIds()) {
				StreamConfiguration sconfig = config.getStreamConfig(streamId);
				streams.put(streamId,(Stream)Class.forName(sconfig.getParameter(StreamConfiguration.CLASS_PATH)).newInstance());
			}
		}catch(Exception e) {
			e.printStackTrace();
			throw new StreamException("Error during streams initialization", e);
		}
		
	}
	
	public class DateUtil
	{
	    public Date addDays(Date date, int days)
	    {
	        Calendar cal = Calendar.getInstance();
	        cal.setTime(date);
	        cal.add(Calendar.DATE, days); //minus number decrements the days
	        return cal.getTime();
	    }
	}
	/**
	 * Class for searching for trending dysco requests 
	 * @author ailiakop
	 *
	 */
	public class TrendingSearchHandler extends Thread {
		
		private LinkedList<DyscoRequest> queue = new LinkedList<DyscoRequest>();
		private Set<String> alreadyInRequests = new HashSet<String>();
		private StreamsMonitor monitor = new StreamsMonitor(streams.size());
		
		private boolean isAlive = true;
		
		
		public TrendingSearchHandler(){
			monitor.addStreams(streams);
			
			logger.info("Streams added to monitor");
		}
		/**
		 * Adds trending dysco requests to  the system
		 */
		public void fetch(){
			
			List<DyscoRequest> requests = fetchTrendingDyscoRequests();
			
			if(requests != null)
				for(DyscoRequest request : requests){
					if(!alreadyInRequests.contains(request.getId())){
						queue.addLast(request);
						alreadyInRequests.add(request.getId());
					
					}
				}

		}
		
		public void run(){
			DyscoRequest request = null;
			while(isAlive){
				
				request = poll();
				if(request == null){
					continue;
				}
				else{
					search(request);
				}
					
			}
		}
		/**
		 * Polls a trending dysco request from the queue
		 * @return
		 */
		private DyscoRequest poll(){
			synchronized (queue) {					
				if (!queue.isEmpty()) {
					DyscoRequest request = queue.removeFirst();
					return request;
				}
				try {
					queue.wait(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				return null;
			}
		}
		/**
		 * Stops TrendingSearchHandler
		 */
		public synchronized void die(){
			isAlive = false;
		}
		/**
		 * Fetches trending requests that have not yet been 
		 * served from database
		 */
		private List<DyscoRequest> fetchTrendingDyscoRequests(){
			
			List<DyscoRequest> dyscoRequests = new ArrayList<DyscoRequest>();
			//add trending attribute
			try{
				dyscoRequests = dyscoRequestDAO.readUnsearchedRequestsByType("trending");
			}catch(Exception e){
				return null;
			}
			
			return dyscoRequests;
		}
		/**
		 * Searches for a trending dysco request
		 * @param request
		 */
		private synchronized void search(DyscoRequest request){
			Integer totalItems = 0; 
			
			List<KeywordsFeed> keywordsFeeds = request.getKeywordsFeeds();
			List<Feed> feeds = new ArrayList<Feed>();
			feeds.addAll(keywordsFeeds);
			
			System.out.println();
			logger.info("Media Searcher handling request : "+request.getId());
			System.out.println();
			
			long t1 = System.currentTimeMillis();
			
			if(feeds != null && !feeds.isEmpty()){
				monitor.startAllStreamsAtOnceWithStandarFeeds(feeds);
				
				while(!monitor.areAllStreamFinished()){
					
				}
				totalItems = monitor.getTotalRetrievedItems();
			}
				
			long t2 = System.currentTimeMillis();
			
			System.out.println();
			logger.info("Request "+request.getId()+" fetched "+totalItems+" total items in "+(t2-t1)+" msecs");
			System.out.println();
			request.setIsSearched(true);
			request.setTotalItems(totalItems);
			dyscoRequestDAO.updateRequest(request);
			
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
		TrendingDyscoHandler trendingHandler;

		public Shutdown(TrendingDyscoHandler trendingHandler) {
			this.trendingHandler = trendingHandler;
		}

		public void run(){
			System.out.println("Shutting down trending dysco handler ...");
			
			try {

				trendingHandler.close();
			} catch (StreamException e) {
				e.printStackTrace();
			}
			
			System.out.println("Done...");
		}
	}
	
	public static void main(String[] args) {
		try {
			
			File configFile;
			
			if(args.length != 1 ) {
				configFile = new File("./conf/search.conf.xml");
				
			}
			else {
				configFile = new File(args[0]);
				
			}
			
			StreamsManagerConfiguration config = StreamsManagerConfiguration.readFromFile(configFile);			
	        
			TrendingDyscoHandler trendingDyscoHandler = new TrendingDyscoHandler(config);
			trendingDyscoHandler.open();
			trendingDyscoHandler.run();
			
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
