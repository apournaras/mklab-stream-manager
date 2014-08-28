package eu.socialsensor.sfc.streams.search;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
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
import eu.socialsensor.framework.common.domain.dysco.Dysco;
import eu.socialsensor.framework.common.domain.dysco.Dysco.DyscoType;
import eu.socialsensor.framework.common.domain.dysco.Message;
import eu.socialsensor.framework.common.domain.dysco.Message.Action;
import eu.socialsensor.sfc.input.DataInputType;
import eu.socialsensor.sfc.input.FeedsCreator;
import eu.socialsensor.sfc.streams.Stream;
import eu.socialsensor.sfc.streams.StreamConfiguration;
import eu.socialsensor.sfc.streams.StreamException;
import eu.socialsensor.sfc.streams.StreamsManagerConfiguration;
import eu.socialsensor.sfc.streams.management.StorageHandler;
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
public class SocialMediaSearcher {
	
	private static String REDIS_HOST = "redis.host";
	private static String REDIS_CHANNEL = "channel";
	
	private static String SOLR_HOST = "solr.hostname";
	private static String SOLR_SERVICE = "solr.service";
	private static String DYSCO_COLLECTION = "dyscos.collection";
	
	public final Logger logger = Logger.getLogger(SocialMediaSearcher.class);
	
	enum MediaSearcherState {
		OPEN, CLOSE
	}
	
	private MediaSearcherState state = MediaSearcherState.CLOSE;
	
	private StreamsManagerConfiguration config = null;
	
	private StorageHandler storageHandler;
	private StreamsMonitor monitor;

	private Jedis jedisClient;
	private Thread listener;
	
	private DyscoRequestHandler dyscoRequestHandler;
	private DyscoRequestReceiver dyscoRequestReceiver;
	
	private DyscoUpdateThread dyscoUpdateThread;
	
	// Handlers of Incoming Dyscos
	private TrendingSearchHandler trendingSearchHandler;
	private CustomSearchHandler customSearchHandler;
	
	private String redisHost;
	private String solrHost;
	private String solrService;
	private String dyscoCollection;
	
	private Map<String, Stream> streams = null;
	
	private BlockingQueue<Dysco> requests = new LinkedBlockingQueue<Dysco>();
	private Queue<String> requestsToDelete = new LinkedBlockingQueue<String>();
	private Queue<Dysco> dyscosToUpdate = new LinkedBlockingQueue<Dysco>();
	
	public SocialMediaSearcher(StreamsManagerConfiguration config) throws StreamException {
		
		if (config == null) {
			throw new StreamException("Manager's configuration must be specified");
		}

		this.config = config;
		
		this.redisHost = config.getParameter(SocialMediaSearcher.REDIS_HOST);
		this.solrHost = config.getParameter(SocialMediaSearcher.SOLR_HOST);
		this.solrService = config.getParameter(SocialMediaSearcher.SOLR_SERVICE);
		this.dyscoCollection = config.getParameter(SocialMediaSearcher.DYSCO_COLLECTION);
		
		//Set up the Streams
		initStreams();
		
		//Set up the Storages
		storageHandler = new StorageHandler(config);
		
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
		
		storageHandler.start();	
		logger.info("Store Manager is ready to store. ");
		
		Set<String> failedStreams = new HashSet<String>();
		for (String streamId : streams.keySet()) {
			try {
				logger.info("MediaSearcher - Start Stream : " + streamId);
				StreamConfiguration sconfig = config.getStreamConfig(streamId);
				Stream stream = streams.get(streamId);
				stream.setHandler(storageHandler);
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
		
		this.dyscoUpdateThread = new DyscoUpdateThread(solrHost, solrService, dyscoCollection, dyscosToUpdate);
		
		this.trendingSearchHandler = new TrendingSearchHandler(monitor, dyscosToUpdate);
		this.customSearchHandler = new CustomSearchHandler(monitor, requestsToDelete);
		
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
                    jedisClient.subscribe(dyscoRequestReceiver, config.getParameter(SocialMediaSearcher.REDIS_CHANNEL));  

                    logger.info("Subscribe returned, closing down");
                    jedisClient.quit();
                } catch (Exception e) {
                	logger.error(e);
                }
            }
        });
		listener.start();
		
		logger.info("Set state to open");
		state = MediaSearcherState.OPEN;
		
		logger.info("Add Shutdown Hook for MediaSeacrher");
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
			logger.info("Close Streams");
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
	
	public synchronized void status() {
		logger.info("=================================================");
		logger.info("MediaSearcherState: " + state);
		logger.info("DyscoRequestHandler.isAlive: " + dyscoRequestHandler.isAlive);
		logger.info("TrendingSearchHandler " + trendingSearchHandler.getState());
		logger.info("CustomSearchHandler " + customSearchHandler.getState());
		logger.info("DyscoUpdateThread " + dyscoUpdateThread.getState());
		
		logger.info("#Requests: " + requests.size());
		logger.info("#RequestsToDelete: " + requestsToDelete.size());
		logger.info("#DyscosToUpdate: " + dyscosToUpdate.size());
		
		logger.info("#Streams: " + streams.size());
		monitor.status();
		trendingSearchHandler.status();
		
		logger.info("=================================================");
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
			this.solrDyscoHandler = SolrDyscoHandler.getInstance(solrHost + "/" + solrService + "/" + dyscoCollection);
		}
		
	    @Override
	    public void onMessage(String channel, String message) {
	    	
	    	logger.info("Received dysco request: " + message);
	    	Message dyscoMessage = Message.create(message);
	    	
	    	String dyscoId = dyscoMessage.getDyscoId();    	
	    	Dysco dysco = null;
	    	synchronized(solrDyscoHandler) {
	    		dysco = solrDyscoHandler.findDyscoLight(dyscoId);
	    	}
	    	
    		if(dysco == null) {
    			logger.error("Invalid dysco request");
    			return;
    		}
	    	
    		Action action = dyscoMessage.getAction();
	    	switch(action) {
		    	case NEW : 
		    		logger.info("New dysco with id: " + dyscoId);
		    		try {
		    			requests.put(dysco);
		    		} catch (InterruptedException e) {
		    			logger.error(e);
		    		}
		    		break;
		    	case UPDATE:
		    		logger.info("Dysco with id: " + dyscoId + " needs update");
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
						synchronized(this) {
							this.wait(10000);
						}
					} catch (InterruptedException e) {
						logger.error(e);
					}
					continue;
				}
				else {
					try {
						logger.info("Process Dysco with id: " + receivedDysco.getId());
						
						FeedsCreator feedsCreator = new FeedsCreator(DataInputType.DYSCO, receivedDysco);
						List<Feed> feeds = feedsCreator.getQuery();
					
						logger.info("Feeds: " + feeds.size() + " created for " + receivedDysco.getId());
						
						DyscoType dyscoType = receivedDysco.getDyscoType();
						logger.info("DyscoType: " + dyscoType);
						
						if(dyscoType.equals(DyscoType.TRENDING)) {
							trendingSearchHandler.addDysco(receivedDysco, feeds);
						}
						else if(dyscoType.equals(DyscoType.CUSTOM)) {
							customSearchHandler.addDysco(receivedDysco, feeds);
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
			Dysco request = null;
			try {
				request = requests.take();
			} catch (Exception e) {
				logger.error(e);
			}
			return request;
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
	 * Class in case system is shutdown 
	 * Responsible to close all services that are running at the time 
	 * @author ailiakop
	 */
	private class Shutdown extends Thread {
		private SocialMediaSearcher searcher = null;

		public Shutdown(SocialMediaSearcher searcher) {
			this.searcher = searcher;
		}

		public void run() {
			logger.info("Shutting down media searcher ...");
			if (searcher != null) {
				try {
					searcher.close();
				} catch (StreamException e) {
					e.printStackTrace();
				}
			}
			logger.info("Done...");
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
			SocialMediaSearcher mediaSearcher = new SocialMediaSearcher(config);
			mediaSearcher.open();
			
			while(mediaSearcher.state == MediaSearcherState.OPEN) {
				try {
					Thread.sleep(60 * 1000);
					mediaSearcher.status();
				} catch (Throwable e) {
					e.printStackTrace();
					// TODO: Handle Error
					break;
				}
			}
			
			System.out.println("==================================================");
			System.out.println("==================================================");
			System.out.println("==================================================");
			System.out.println("==================================================");
			System.out.println("==================================================");
			System.out.println("==================================================");
			System.out.println("==================================================");
			System.out.println("==================================================");
			
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
