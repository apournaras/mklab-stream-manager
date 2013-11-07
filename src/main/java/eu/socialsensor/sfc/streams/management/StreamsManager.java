package eu.socialsensor.sfc.streams.management;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;


import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.streams.Stream;
import eu.socialsensor.framework.streams.StreamConfiguration;
import eu.socialsensor.framework.streams.StreamException;
import eu.socialsensor.sfc.streams.StreamsManagerConfiguration;
import eu.socialsensor.sfc.streams.input.FeedsCreatorImpl.ConfigFeedsCreator;


/**
 * Thread-safe class for retrieving content according to 
 * keywords - user - location feeds from social networks
 * (Twitter,Youtube,Facebook,Flickr,Instagram,Tumblr,GooglePlus)
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class StreamsManager{
	protected static final String REQUEST_PERIOD = "period";
	
	public final Logger logger = Logger.getLogger(StreamsManager.class);
	
	enum ManagerState {
		OPEN, CLOSE
	}

	private Map<String, Stream> streams = null;
	private Map<String, List<Feed>> feedsOfStream = new HashMap<String,List<Feed>>();
	private StreamsManagerConfiguration config = null;
	private StoreManager storeManager;
	private ConfigFeedsCreator configFeedsCreator;
	private ManagerState state = ManagerState.CLOSE;
	private int numberOfConsumers = 5; //for multi-threaded items' storage
	private long requestPeriod;
	private Set<String> streamConfigs;
	private List<Feed> feeds = new ArrayList<Feed>();
	private boolean isAlive = true;
	
	public StreamsManager(StreamsManagerConfiguration config) throws StreamException {

		if (config == null) {
			throw new StreamException("Manager's configuration must be specified");
		}

		this.config = config;
		
		streamConfigs = config.getStreamIds();
		
		requestPeriod = Long.parseLong(config.getParameter(StreamsManager.REQUEST_PERIOD,"3600")) * 1000;  //convert in milliseconds

		initStreams();
		
		Runtime.getRuntime().addShutdownHook(new Shutdown(this));
	}
	/**
	 * Opens Manager by starting the auxiliary modules and setting up
	 * the database for reading/storing
	 * @throws StreamException
	 */
	public synchronized void open() throws StreamException {
		
		if (state == ManagerState.OPEN) {
			return;
		}
		state = ManagerState.OPEN;
		logger.info("Streams are now open");
		
		try {

			storeManager = new StoreManager(config,numberOfConsumers);
			storeManager.start();	
			
			for (String streamId : streamConfigs) {
				StreamConfiguration sconfig = config.getStreamConfig(streamId);
				Stream stream = streams.get(streamId);
				stream.setHandler(storeManager);
				stream.open(sconfig);
				
				//start tracking
				configFeedsCreator = new ConfigFeedsCreator(sconfig);
				configFeedsCreator.extractKeywords();
				
				feeds = configFeedsCreator.createFeeds();
				feedsOfStream.put(streamId, feeds);
			}
	
		}catch(Exception e) {
			e.printStackTrace();
			throw new StreamException("Error during streams open", e);
		}
	}
	
	public synchronized void search(){
		long currentTime = System.currentTimeMillis();
		long timeOfSearch = currentTime + requestPeriod; 
		
		if(feeds == null){
			System.out.println("No feeds to search");
			return;
		}
	
		if(!streams.containsKey("Twitter")){
			while(isAlive){
				//Periodically perform polling
				if(Math.abs(currentTime - timeOfSearch)>= requestPeriod){
					for(String streamId : streamConfigs){
						Stream stream = streams.get(streamId);
						try {
							stream.search(feedsOfStream.get(streamId));
						} catch (StreamException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
							
					}
					timeOfSearch = currentTime;
				}
				currentTime = System.currentTimeMillis();
			}
		}
		else{
			//Twitter works as subscriber to channel
			try {
				streams.get("Twitter").search(feedsOfStream.get("Twitter"));
			} catch (StreamException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
	}
	
	/**
	 * Closes Manager along with its auxiliary modules
	 * @throws StreamException
	 */
	public synchronized void close() throws StreamException {
		
		if (state == ManagerState.CLOSE) {
			return;
		}
		isAlive = false;
		try{
			for (Stream stream : streams.values()) {
				stream.close();
			}
			
			if (storeManager != null) {
				storeManager.stop();
			}
			
			state = ManagerState.CLOSE;
		}catch(Exception e) {
			throw new StreamException("Error during streams close",e);
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
	 * Class in case system is shutdown 
	 * Responsible to close all services 
	 * that are running at the time being
	 * @author ailiakop
	 *
	 */
	
	private class Shutdown extends Thread {
		StreamsManager manager = null;

		public Shutdown(StreamsManager manager) {
			this.manager = manager;
		}

		public void run() {
			System.out.println("Shutting down stream manager...");
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
	
	public static void main(String[] args) {
		try {
			
			File configFile;
			
			if(args.length != 1 ) {
				configFile = new File("./conf/streams.conf.xml");
				
			}
			else {
				configFile = new File(args[0]);
			
			}
			
			StreamsManagerConfiguration config = StreamsManagerConfiguration.readFromFile(configFile);		
				
	        
			StreamsManager streamsManager = new StreamsManager(config);
			streamsManager.open();
			streamsManager.search();
		
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
