package eu.socialsensor.sfc.streams.management.Handlers;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
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
 * Class for handling custom dysco requests
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class CustomDyscoHandler{
	private static String HOST = "mongodb.host";
	private static String DATABASE = "mongodb.database";
	private static String DYSCO_REQUESTS = "mongodb.dyscos.collection";
	private static String timeframe;
	private static String frequency;
	
	Logger logger = Logger.getLogger(CustomDyscoHandler.class);
	
	enum CustomDyscoHandlerState {
		OPEN, CLOSE
	}
	
private CustomDyscoHandlerState state = CustomDyscoHandlerState.CLOSE;
	
	private Map<String, Stream> streams = null;
	private StreamsManagerConfiguration config = null;
	
	private DyscoRequestDAO dyscoRequestDAO;
	private String dyscoRequestsCollectionName;

	private StoreManager storeManager;
	private CustomSearchHandler customSearchHandler;
	
	private String host;
	private String dyscoDBName;
	
	public CustomDyscoHandler(StreamsManagerConfiguration config) 
			throws StreamException {
		
		if (config == null) {
			throw new StreamException("Manager's configuration must be specified");
		}
		
		this.config = config;
		
		StorageConfiguration storage_config = config.getStorageConfig("Mongodb");
		this.host = storage_config.getParameter(CustomDyscoHandler.HOST);
		this.dyscoDBName = storage_config.getParameter(CustomDyscoHandler.DATABASE);
		this.dyscoRequestsCollectionName = storage_config.getParameter(CustomDyscoHandler.DYSCO_REQUESTS, "Dyscos");
		
		logger.info("Open MongoDB storage <host: " + host + ", database: " + dyscoDBName + 
				", items collection: " + dyscoRequestsCollectionName +">");
		
		this.dyscoRequestDAO = new DyscoRequestDAOImpl(host, dyscoDBName, dyscoRequestsCollectionName);
		
		initStreams();
		
		customSearchHandler = new CustomSearchHandler(); 
		
        Runtime.getRuntime().addShutdownHook(new Shutdown(this));
       
	}
	
	/**
	 * Opens Manager by starting the auxiliary modules and setting up
	 * the database for reading/storing
	 * @throws StreamException
	 */
	public void open() throws StreamException {	
		
		if (state == CustomDyscoHandlerState.OPEN){
			return;
		}
		
		storeManager = new StoreManager(config);
		storeManager.start();
		
		for (String streamId : config.getStreamIds()) {
			StreamConfiguration sconfig = config.getStreamConfig(streamId);
			Stream stream = streams.get(streamId);
			stream.setHandler(storeManager);
			stream.open(sconfig);
			if(!stream.setMonitor()){
				System.err.println("Feeds' monitor could not be set for stream : "+streamId);
				return;
			}
		}
		
		state = CustomDyscoHandlerState.OPEN;
		
		customSearchHandler.start();
		
	}
	
	/**
	 * Constantly fetches new custom dysco requests and
	 * adds them to the queue to be served. 
	 */
	public void run(){
		while(state == CustomDyscoHandlerState.OPEN){
			
			customSearchHandler.fetch();
			
			customSearchHandler.updateQueue();
		}
		
		logger.info("Custom Search Handler has stoped fetching");
	}
	
	/**
	 * Closes Manager along with its auxiliary modules
	 * @throws StreamException
	 */
	public void close() throws StreamException {
		if (state == CustomDyscoHandlerState.CLOSE){
			return;
		}
		
		try{
			for (Stream stream : streams.values()){
				stream.close();
			}
			
			if(customSearchHandler != null)
				customSearchHandler.die();
			
			state = CustomDyscoHandlerState.CLOSE;
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
	 * Class for searching for custom dysco requests 
	 * @author ailiakop
	 *
	 */
	public class CustomSearchHandler extends Thread {
		
		private LinkedList<DyscoRequest> queue = new LinkedList<DyscoRequest>();
		private Set<String> alreadyInRequests = new HashSet<String>();
		private StreamsMonitor monitor = new StreamsMonitor(streams.size());
		private Map<DyscoRequest,Long> requestsLifetime = new HashMap<DyscoRequest,Long>();
		
		private boolean isAlive = true;
		
		private long dyscoTimeframe = Long.parseLong(timeframe);//48 * 3600000; //two days
		private long freq = Long.parseLong(frequency);//2 * 300000; //ten minutes
		
		public CustomSearchHandler(){
			
			monitor.addStreams(streams);
			
			logger.info("Streams added to monitor");
			
		}
		
		/**
		 * Adds custom dysco requests to  the system
		 */
		public void fetch(){

			List<DyscoRequest> requests = fetchCustomDyscoRequests();
		
			if(requests != null)
				for(DyscoRequest request : requests){
					if(!alreadyInRequests.contains(request.getId())){
						queue.addLast(request);
						alreadyInRequests.add(request.getId());
						requestsLifetime.put(request, System.currentTimeMillis());
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
		 * Polls a custom dysco request from the queue
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
		 * Stops CustomSearchHandler
		 */
		public synchronized void die(){
			isAlive = false;
		}
		/**
		 * Fetches custom requests that have not yet been 
		 * served from database
		 */
		private List<DyscoRequest> fetchCustomDyscoRequests(){
			
			List<DyscoRequest> dyscoRequests = new ArrayList<DyscoRequest>();
			//add trending attribute
			
			try{
				dyscoRequests = dyscoRequestDAO.readUnsearchedRequestsByType("custom");
			}catch(Exception e){
				return null;
			}
			
			return dyscoRequests;
		}
		/**
		 * Updates the queue of custom dyscos' requests and re-examines or deletes 
		 * requests according to their time in the system
		 */
		private synchronized void updateQueue(){
			
			List<DyscoRequest> requestsToRemove = new ArrayList<DyscoRequest>();
			long currentTime = System.currentTimeMillis();
			
			for(Map.Entry<DyscoRequest, Long> entry : requestsLifetime.entrySet()){
			//	System.out.println("Checking dysco : "+entry.getKey().getId()+" that has time in system : "+(currentTime - entry.getValue())/1000);
				
				if(currentTime - entry.getValue() > freq){
					
					entry.setValue(currentTime);
					DyscoRequest requestToSearch = entry.getKey();
					queue.addLast(requestToSearch);
					System.out.println("Dysco "+requestToSearch.getId()+" added to queue again");
					System.out.println("I am going now to search again "+entry.getKey().getId()+" - TotalTime in the system : "+(currentTime - entry.getKey().getTimestamp().getTime())/1000);
					if(currentTime - entry.getKey().getTimestamp().getTime()> dyscoTimeframe){
						
						requestsToRemove.add(entry.getKey());
					}
						
				}
				
				if(dyscoRequestDAO.getDyscoRequest(entry.getKey().getId()).getIsSearched()){
					
					queue.remove(entry.getKey());
					
					requestsToRemove.add(entry.getKey());
				}
			}
			
			if(!requestsToRemove.isEmpty()){
				System.out.println("Remove request");
				for(DyscoRequest requestToRemove : requestsToRemove){
					requestsLifetime.remove(requestToRemove);
					alreadyInRequests.remove(requestToRemove.getId());
					requestToRemove.setIsSearched(true);
					dyscoRequestDAO.updateRequest(requestToRemove);
				}
				requestsToRemove.clear();	
			}
			
		}
		
		/**
		 * Searches for a custom dysco request
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
				totalItems = monitor.getTotalRetrievedItems().size();
			}
				
			long t2 = System.currentTimeMillis();
			
			System.out.println();
			logger.info("Request "+request.getId()+" fetched "+totalItems+" total items in "+(t2-t1)+" msecs");
			System.out.println();
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
		CustomDyscoHandler customHandler;
		public Shutdown(CustomDyscoHandler customHandler) {
			this.customHandler = customHandler;
		}

		public void run(){
			System.out.println("Shutting down custom dysco handler...");
			
			try {
				storeManager.stop();
				customHandler.close();
			} catch (StreamException e) {
				e.printStackTrace();
			}
			
			System.out.println("Done...");
		}
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
		try {
			
			File configFile;
			
			if(args.length != 1 ) {
				configFile = new File("./conf/search.conf.xml");
				System.out.println("Timeframe to search custom dysco : [3,600,000 is 1 hour]");
				timeframe = bufferRead.readLine();
				
				System.out.println("Frequency to search custom dysco : [300,000 is 5 minutes]");
				frequency = bufferRead.readLine();
			}
			else {
				configFile = new File(args[0]);
				
			}
			
			StreamsManagerConfiguration config = StreamsManagerConfiguration.readFromFile(configFile);			
	        
			CustomDyscoHandler customDyscoHandler = new CustomDyscoHandler(config);
			customDyscoHandler.open();
			customDyscoHandler.run();
			
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
