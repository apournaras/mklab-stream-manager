package eu.socialsensor.sfc.streams.management;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import eu.socialsensor.framework.client.search.solr.SolrDyscoHandler;
import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.common.domain.dysco.Dysco;
import eu.socialsensor.framework.streams.Stream;
import eu.socialsensor.framework.streams.StreamConfiguration;
import eu.socialsensor.framework.streams.StreamException;
import eu.socialsensor.sfc.streams.StreamsManagerConfiguration;
import eu.socialsensor.sfc.streams.monitors.StreamsMonitor;

public class MediaSearcher {
	private static String HOST = "redis.host";
	private static String DYSCO_COLLECTION = "dysco.collection";
	
	public final Logger logger = Logger.getLogger(StreamsManager.class);
	
	enum ManagerState {
		OPEN, CLOSE
	}

	private StreamsManagerConfiguration config = null;
	private StoreManager storeManager;
	private StreamsMonitor monitor;
	private ManagerState state = ManagerState.CLOSE;
	private Jedis subscriberJedis;
	private DyscoRequestHandler dyscoRequestHandler;
	private DyscoRequestReceiver dyscoRequestReceiver;
	
	private String host;
	private String dyscoCollection;
	
	private int numberOfConsumers = 1; //for multi-threaded items' storage
	
	private Map<String, Stream> streams = null;
	
	private List<Feed> feeds = new ArrayList<Feed>();
	
	private Queue<Dysco> requests = new LinkedList<Dysco>();
	
	
	public MediaSearcher(StreamsManagerConfiguration config) throws StreamException{
		if (config == null) {
			throw new StreamException("Manager's configuration must be specified");
		}

		this.config = config;
		this.host = config.getParameter(HOST);
		this.dyscoCollection = config.getParameter(DYSCO_COLLECTION);
		
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
		if (state == ManagerState.OPEN) {
			return;
		}
		state = ManagerState.OPEN;
		logger.info("Streams are now open");
		
		this.dyscoRequestHandler = new DyscoRequestHandler();
		this.dyscoRequestReceiver = new DyscoRequestReceiver();
		JedisPoolConfig poolConfig = new JedisPoolConfig();
        JedisPool jedisPool = new JedisPool(poolConfig, host, 6379, 0);
        this.subscriberJedis = jedisPool.getResource();
        

		new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                	logger.info("Try to subscribe to redis");
                    subscriberJedis.subscribe(dyscoRequestReceiver,eu.socialsensor.framework.client.search.MediaSearcher.CHANNEL);
                   
                } catch (Exception e) {
                }
            }
        }).start();
		
		state = ManagerState.OPEN;
	}
	
	/**
	 * Closes Manager along with its auxiliary modules
	 * @throws StreamException
	 */
	public synchronized void close() throws StreamException {
		
		if (state == ManagerState.CLOSE) {
			return;
		}
		
		try{
			for (Stream stream : streams.values()) {
				stream.close();
			}
			
			if (storeManager != null) {
				storeManager.stop();
			}
			
			if(dyscoRequestReceiver != null){
				dyscoRequestReceiver.close();
			}
			
			if(dyscoRequestHandler != null){
				dyscoRequestHandler.close();
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
	 * Class for handling incoming dysco requests that are received with redis
	 * @author ailiakop
	 *
	 */
	private class DyscoRequestHandler extends Thread {

		private boolean isAlive = true;
		
		public DyscoRequestHandler(){
			
		}
		
		public void run(){
			Dysco receivedDysco = null;
			while(true){
				receivedDysco = poll();
				if(receivedDysco == null){
					continue;
				}
				else{
					//Call Query Builder
				}
			}
		}
		
		/**
		 * Polls a trending dysco request from the queue
		 * @return
		 */
		private Dysco poll(){
			synchronized (requests) {					
				if (!requests.isEmpty()) {
					Dysco request = requests.remove();
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
		
		public void close(){
			isAlive = true;
		}
	}
	
	public class DyscoRequestReceiver extends JedisPubSub{

		private SolrDyscoHandler solrdyscoHandler;
		
		public DyscoRequestReceiver(){
			
			this.solrdyscoHandler = SolrDyscoHandler.getInstance(dyscoCollection);
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
	    	
	    	Dysco dysco = solrdyscoHandler.findDyscoLight(message);
	    	
	    	if(dysco.getSolrQuery() == null){
	    		requests.add(dysco);
		    	
		    	logger.info("Dysco "+message+" stored!");
		    	
	    	}
	    	else{
	    		
				logger.info("Dysco :"+message+" to delete...");
				
				/**
				 * TO DO : Check what happens if current dysco is being searched at the moment.
				 */
				
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
	    	subscriberJedis.quit();
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
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
