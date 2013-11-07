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
import org.joda.time.DateTime;
import org.xml.sax.SAXException;

import eu.socialsensor.framework.client.dao.ItemDAO;
import eu.socialsensor.framework.client.dao.impl.ItemDAOImpl;
import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.streams.Stream;
import eu.socialsensor.framework.streams.StreamConfiguration;
import eu.socialsensor.framework.streams.StreamException;
import eu.socialsensor.sfc.streams.StorageConfiguration;
import eu.socialsensor.sfc.streams.StreamsManagerConfiguration;
import eu.socialsensor.sfc.streams.input.FeedsCreatorImpl.SimpleFeedsCreator;
import eu.socialsensor.sfc.streams.management.StoreManager;
import eu.socialsensor.sfc.streams.monitors.StreamsMonitor;

/**
 * Class for handling rss items
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class RSSHandler{
	
	private static String PRIVATE_HOST = "mongodb.private.host";
	private static String RSS_ITEMS = "mongodb.rss.collection";
	
	Logger logger = Logger.getLogger(RSSHandler.class);
	
	enum RSSHandlerState {
		OPEN, CLOSE
	}
	
	private RSSHandlerState state = RSSHandlerState.CLOSE;
	
	private Map<String, Stream> streams = null;
	private StreamsManagerConfiguration config = null;
	
	private ItemDAO itemDAO;
	private String rssCollectionName;
	
	private StoreManager storeManager;
	private RSSSearchHandler rssSearchHandler;
	private RSSUpdator rssUpdator;
	
	private String private_host;
	private String rssDBName;
	
	
	public RSSHandler(StreamsManagerConfiguration config) 
			throws StreamException {
		
		if (config == null) {
			throw new StreamException("RSS Handler's configuration must be specified");
		}
		
		this.config = config;
		
		StorageConfiguration storage_config = config.getStorageConfig("Mongodb");
		this.private_host = storage_config.getParameter(RSSHandler.PRIVATE_HOST);
		this.rssCollectionName = storage_config.getParameter(RSSHandler.RSS_ITEMS, "rssItems");
		
		DateTime currentDateTime = new DateTime();
		
		rssDBName = "RSS_"+currentDateTime.getDayOfMonth()+"_"+currentDateTime.getMonthOfYear()+"_"+currentDateTime.getYear();
		this.itemDAO = new ItemDAOImpl(private_host, rssDBName, rssCollectionName);
		
		logger.info("Open MongoDB storage <host: " + private_host + ", database: " + rssDBName + 
				", items collection: " + rssCollectionName +">");
		
		
		initStreams();
		
		rssSearchHandler = new RSSSearchHandler(); 
		rssUpdator = new RSSUpdator();
		
        Runtime.getRuntime().addShutdownHook(new Shutdown(this));
       
	}
	
	/**
	 * Opens Manager by starting the auxiliary modules and setting up
	 * the database for reading/storing
	 * @throws StreamException
	 */
	public void open() throws StreamException {	
		
		if (state == RSSHandlerState.OPEN){
			return;
		}
		
		storeManager = new StoreManager(config);
		storeManager.start();
		
		for (String streamId : config.getStreamIds()) {
			StreamConfiguration sconfig = config.getStreamConfig(streamId);
			Stream stream = streams.get(streamId);
			stream.open(sconfig);
			stream.setHandler(storeManager);
			if(!stream.setMonitor()){
				System.err.println("Feeds' monitor could not be set for stream : "+streamId);
				return;
			}
		}
		
		state = RSSHandlerState.OPEN;
		
		rssUpdator.start();
		rssSearchHandler.start();
		
		run();
	}
	
	/**
	 * Examines if there are new rss items in the system and adds them
	 * to the queue for searching
	 */
	public void run(){
		while(state == RSSHandlerState.OPEN){
			
			rssSearchHandler.addRSSItem();
			
		}
		
		logger.info("Custom Search Handler has stoped fetching");
	}
	
	/**
	 * Closes Manager along with its auxiliary modules
	 * @throws StreamException
	 */
	public void close() throws StreamException {
		if (state == RSSHandlerState.CLOSE){
			return;
		}
		
		try{
			for (Stream stream : streams.values()){
				stream.close();
			}
			if (storeManager != null) {
				storeManager.stop();
			}
			
			if(rssSearchHandler != null)
				rssSearchHandler.die();
			
			if(rssUpdator != null)
				rssUpdator.die();
			
			state = RSSHandlerState.CLOSE;
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
	 * Class for the constant update of rss feeds in the system. 
	 * Updates the collection of the rss feeds every one hour.
	 * @author ailiakop
	 *
	 */
	private class RSSUpdator extends Thread {
		private long oneHour = 3600000;
		private int previousDay = 0;
		private boolean isAlive = true;
		
		public RSSUpdator(){
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
			String currentDbName = "RSS_"+currentDateTime.getDayOfMonth()+"_"+currentDateTime.getMonthOfYear()+"_"+currentDateTime.getYear();
			System.out.println("Reading "+currentDbName+" DB");
			
			if(previousDay == 0)
				previousDay = currentDateTime.getDayOfMonth();
			else if(previousDay != currentDateTime.getDayOfMonth())
				itemDAO.deleteDB();
			
			itemDAO = new ItemDAOImpl(private_host, currentDbName,rssCollectionName);
			previousDay = currentDateTime.getDayOfMonth();
		}
		
		public void die(){
			isAlive = false;
		}
	}
	
	/**
	 *  Class for searching for rss items
	 * @author ailiakop
	 *
	 */
	private class RSSSearchHandler extends Thread{
		private LinkedList<RSSEntry<Item,List<Feed>>> queue = new LinkedList<RSSEntry<Item,List<Feed>>>();
		private Set<String> alreadyInItems = new HashSet<String>();
		private StreamsMonitor monitor = new StreamsMonitor(streams.size());
		private SimpleFeedsCreator s_feedsCreator;
		
		boolean isAlive = true;
		
		public RSSSearchHandler(){
			monitor.addStreams(streams.values());
			
			logger.info("Streams added to monitor");
		}
		
		public synchronized void addRSSItem(){
			List<Item> rssItems = fetchRSSItems();
			
			for(Item rss : rssItems){
				
				if(!alreadyInItems.contains(rss.getId()) && !rss.getIsSearched()){
					alreadyInItems.add(rss.getId());
					s_feedsCreator= new SimpleFeedsCreator(rss.getPublicationDate());
					s_feedsCreator.filterContent(rss.getKeywords(), rss.getEntities());
					
					s_feedsCreator.extractKeywords();
					
					List<Feed> feeds = s_feedsCreator.createFeeds();
					RSSEntry<Item,List<Feed>> entry = new RSSEntry<Item,List<Feed>>(rss,feeds);
					
					queue.add(entry);
					
				}
			}
		}
		
		public void run(){
			RSSEntry<Item,List<Feed>> input = null;
			while(isAlive){
				
				input = poll();
				if(input == null){
					continue;
				}
				else{
					search(input);
					
				}
					
			}
		}
		/**
		 * Polls an rss entry from the queue
		 * @return
		 */
		private RSSEntry<Item,List<Feed>> poll(){
			synchronized (queue) {					
				if (!queue.isEmpty()) {
					RSSEntry<Item,List<Feed>> input = queue.removeFirst();
					return input;
				}
				try {
					queue.wait(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				return null;
			}
		}
		
		public synchronized void die(){
			isAlive = false;
		}

		/**
		 * Searches for an rssEntry
		 * @param request
		 */
		private void search(RSSEntry<Item,List<Feed>> input){
			Integer totalItems = 0; 
			
			long t1 = System.currentTimeMillis();
			
			List<Feed> feeds = input.getValue();
			Item rssItem = input.getKey();
			System.out.println("Searching for "+rssItem.getId()+" with "+feeds.size()+" feeds");
			if(feeds != null && !feeds.isEmpty()){
				monitor.start(feeds);
				
				while(!monitor.isMonitorFinished()){
					
				}
				totalItems = monitor.getTotalRetrievedItems();
			}
				
			long t2 = System.currentTimeMillis();
			
			System.out.println();
			logger.info("RSS Item "+rssItem.getId()+" fetched "+totalItems+" total items in "+(t2-t1)+" msecs");
			System.out.println();
			rssItem.setIsSearched(true);
			itemDAO.updateItem(rssItem);
			
		}
		
		/**
		 * Fetches rss items that have not yet been 
		 * served from database
		 */
		private List<Item> fetchRSSItems(){
			
			List<Item> rssItems = new ArrayList<Item>();
			//add trending attribute
			rssItems = itemDAO.readItemsByStatus();
			
			return rssItems;
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
		RSSHandler rssHandler;
		
		public Shutdown(RSSHandler rssHandler) {
			this.rssHandler = rssHandler;
		}

		public void run(){
			System.out.println("Shutting down trending dysco handler ...");
			
			try {
				
				rssHandler.close();
			} catch (StreamException e) {
				e.printStackTrace();
			}
			
			System.out.println("Done...");
		}
	}
	
	/**
	 * Class for an RSSEntry that is the rssItem to be examined 
	 * along with the feeds with which relevant content will be retrieved
	 * @author ailiakop
	 *
	 * @param <K>
	 * @param <V>
	 */
	final class RSSEntry<K, V> implements Map.Entry<K, V> {
	    private final K key;
	    private V value;

	    public RSSEntry(K key, V value) {
	        this.key = key;
	        this.value = value;
	    }

	    @Override
	    public K getKey() {
	        return key;
	    }

	    @Override
	    public V getValue() {
	        return value;
	    }

	    @Override
	    public V setValue(V value) {
	        V old = this.value;
	        this.value = value;
	        return old;
	    }
	}
	/**
	 * @param args
	 */
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
	        
			RSSHandler rssHandler = new RSSHandler(config);
			rssHandler.open();
			rssHandler.run();
			
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
