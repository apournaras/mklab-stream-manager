package eu.socialsensor.sfc.streams.management;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;


import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.streams.Stream;
import eu.socialsensor.framework.streams.StreamConfiguration;
import eu.socialsensor.framework.streams.StreamException;
import eu.socialsensor.sfc.builder.FeedsCreator;
import eu.socialsensor.sfc.builder.InputConfiguration;
import eu.socialsensor.sfc.builder.input.DataInputType;
import eu.socialsensor.sfc.streams.StreamsManagerConfiguration;
import eu.socialsensor.sfc.streams.monitors.StreamsMonitor;

public class NewsAggregator {
	
	public final Logger logger = Logger.getLogger(NewsAggregator.class);
	
	enum NewsAggregatorState{
		OPEN, CLOSE
	}
	
	private Map<String, Stream> streams = null;
	private StreamsManagerConfiguration config = null;
	private InputConfiguration input_config = null;
	private StoreManager storeManager;
	private StreamsMonitor monitor;
	private NewsAggregatorState newsAggregatorState = NewsAggregatorState.CLOSE;
	
	private Eliminator eliminator;
	
	private int numberOfConsumers = 1; //for multi-threaded items' storage

	private List<Feed> feeds = new ArrayList<Feed>();
	
	public NewsAggregator(StreamsManagerConfiguration config,InputConfiguration input_config) throws StreamException{
		if (config == null || input_config == null) {
			throw new StreamException("News Aggregator's configuration must be specified");
		}
		
		//Set the configuration files
		this.config = config;
		this.input_config = input_config;
		//Set up the Streams
		initStreams();
		//If there are Streams to monitor start the StreamsMonitor
		if(streams != null && !streams.isEmpty()){
			monitor = new StreamsMonitor(streams.size());
		}
		
		Runtime.getRuntime().addShutdownHook(new Shutdown(this));
	}
	
	/**
	 * Opens Manager by starting the auxiliary modules and setting up
	 * the database for reading/storing
	 * @throws StreamException
	 */
	public synchronized void open() throws StreamException {
		
		if (newsAggregatorState == NewsAggregatorState.OPEN) {
			return;
		}
		newsAggregatorState = NewsAggregatorState.OPEN;
		logger.info("Streams are now open");
		
		try {
			//Start store Manager 
			storeManager = new StoreManager(config, numberOfConsumers);
			storeManager.start();	
			logger.info("Store Manager is ready to store.");

			FeedsCreator feedsCreator = new FeedsCreator(DataInputType.TXT_FILE,input_config);
			Map<String,List<Feed>> results = feedsCreator.getQueryPerStream();
			
			//Start the Streams
			for (String streamId : streams.keySet()) {
				logger.info("Stream Manager - Start Stream : "+streamId);
				StreamConfiguration sconfig = config.getStreamConfig(streamId);
				Stream stream = streams.get(streamId);
				stream.setHandler(storeManager);
				stream.open(sconfig);
				
				feeds = results.get(streamId);
				
				if(feeds == null || feeds.isEmpty()){
					logger.error("No feeds for Stream : "+streamId);
					logger.error("Close Stream : "+streamId);
					stream.close();
					continue;
				}
				
				monitor.addStream(streamId, stream, feeds);
				monitor.startStream(streamId);
			}
			
			if(monitor != null && monitor.getNumberOfStreamFetchTasks() > 0){
				monitor.startReInitializer();
			}
			
			eliminator = new Eliminator(this);
			eliminator.start();

		}catch(Exception e) {
			e.printStackTrace();
			throw new StreamException("Error during streams open", e);
		}
	}
	
	
	/**
	 * Initializes the streams that correspond to the news feeds collectors 
	 * that are used for news feed retrieval
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
	 * Closes NewsAggregator along with its auxiliary modules
	 * @throws StreamException
	 */
	public synchronized void close() throws StreamException {
		
		if (newsAggregatorState == NewsAggregatorState.CLOSE) {
			return;
		}
		
		try{
			for (Stream stream : streams.values()) {
				stream.close();
			}
			
			if (storeManager != null) {
				storeManager.stop();
			}
			
			newsAggregatorState = NewsAggregatorState.CLOSE;
		}catch(Exception e) {
			throw new StreamException("Error during streams close", e);
		}
	}
	

	private class Eliminator extends Thread {
		private NewsAggregator aggregator = null;
		private long checkTime = 60000 * 60 * 24; //1-day 
		private long lastCheck = System.currentTimeMillis();
		private long currentTime = System.currentTimeMillis();
		private long dateThreshold = 60000 * 60 * 24 * 30; //1-week
		
		public Eliminator(NewsAggregator aggregator) {
			this.aggregator = aggregator;
		}

		public void run() {
			logger.info("Eliminator started");
			while(aggregator.newsAggregatorState.equals(NewsAggregatorState.OPEN)){
				
				while(Math.abs(currentTime - lastCheck) < checkTime){
					currentTime = System.currentTimeMillis();
				}
				
				storeManager.deleteItemsOlderThan(dateThreshold);
				lastCheck = System.currentTimeMillis();
			}
			
			
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
		NewsAggregator aggregator = null;

		public Shutdown(NewsAggregator aggregator) {
			this.aggregator = aggregator;
		}

		public void run() {
			System.out.println("Shutting down news aggregator...");
			if (aggregator != null) {
				try {
					aggregator.close();
				} catch (StreamException e) {
					e.printStackTrace();
				}
			}
			System.out.println("Done...");
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			
			File streamConfigFile;
			
			
			if(args.length != 1) {
				streamConfigFile = new File("./conf/newsfeed.conf.xml");
				
				
			}
			else {
				streamConfigFile = new File(args[0]);
				
			}
			
			StreamsManagerConfiguration config = StreamsManagerConfiguration.readFromFile(streamConfigFile);		
			InputConfiguration input_config = InputConfiguration.readFromFile(streamConfigFile);		
			
			NewsAggregator streamsManager = new NewsAggregator(config,input_config);
			streamsManager.open();
		
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
