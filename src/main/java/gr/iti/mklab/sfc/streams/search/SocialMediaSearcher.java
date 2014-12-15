package gr.iti.mklab.sfc.streams.search;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import gr.iti.mklab.framework.client.search.solr.SolrDyscoHandler;
import gr.iti.mklab.framework.common.domain.Message;
import gr.iti.mklab.framework.common.domain.Message.Action;
import gr.iti.mklab.framework.common.domain.dysco.Dysco;
import gr.iti.mklab.sfc.streams.Stream;
import gr.iti.mklab.sfc.streams.StreamConfiguration;
import gr.iti.mklab.sfc.streams.StreamException;
import gr.iti.mklab.sfc.streams.StreamsManagerConfiguration;
import gr.iti.mklab.sfc.streams.management.StorageHandler;
import gr.iti.mklab.sfc.streams.monitors.StreamsMonitor;

/**
 * Class responsible for searching media content in social networks
 * (Twitter, YouTube, Facebook, Google+, Instagram, Flickr, Tumblr)
 * give a DySco as input. The retrieval of relevant content is based
 * on queries embedded to the DySco. DyScos are received as messages 
 * via Redis service and can be both custom or trending. 
 * 
 * @author ailiakop
 * @email ailiakop@iti.gr
 */
public class SocialMediaSearcher extends Thread {
	
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
	
	private DyscoRequestHandler dyscoRequestHandler;
	private DyscoRequestReceiver dyscoRequestReceiver;
	
	// Handlers of Incoming Dyscos
	private TrendingSearchHandler trendingSearchHandler;
	private CustomSearchHandler customSearchHandler;
	
	private QueryExpander queryExpander;
	
	private String redisHost;
	private String redisChannel;
	private String solrHost;
	private String solrService;
	private String dyscoCollection;
	
	private Map<String, Stream> streams = null;
	
	private SolrDyscoHandler solrdyscoHandler = null;
	
	public SocialMediaSearcher(StreamsManagerConfiguration config) throws StreamException {
		
		if (config == null) {
			throw new StreamException("Manager's configuration must be specified");
		}

		this.config = config;
		
		this.redisHost = config.getParameter(SocialMediaSearcher.REDIS_HOST);
		this.redisChannel = config.getParameter(SocialMediaSearcher.REDIS_CHANNEL);
		
		this.solrHost = config.getParameter(SocialMediaSearcher.SOLR_HOST);
		this.solrService = config.getParameter(SocialMediaSearcher.SOLR_SERVICE);
		this.dyscoCollection = config.getParameter(SocialMediaSearcher.DYSCO_COLLECTION);
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
		
		//Set up the Streams
		initStreams();
		
		//Set up the Storages
		storageHandler = new StorageHandler(config);
		
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
				logger.error("Stream " + streamId + " failed to open.");
				failedStreams.add(streamId);
			}
		}
		
		// Remove streams failed to open
		for (String streamId : failedStreams) {
			streams.remove(streamId);
		}
		logger.info(streams.size() + " streams are now open");
		
		//If there are Streams to monitor start the StreamsMonitor
		if(streams != null && !streams.isEmpty()) {
			monitor = new StreamsMonitor(streams.size());
			monitor.addStreams(streams);
			logger.info("Streams added to monitor");
		}
		else {
			logger.error("Streams Monitor cannot be started");
		}
		
		String solrServiceUrl = solrHost + "/" + solrService + "/" + dyscoCollection;
		
		solrdyscoHandler = SolrDyscoHandler.getInstance(solrServiceUrl);
		
		//start handlers
		dyscoRequestReceiver = new DyscoRequestReceiver(redisHost, redisChannel);
		dyscoRequestHandler = new DyscoRequestHandler(solrServiceUrl);
		
		queryExpander  = new QueryExpander();
		
		trendingSearchHandler = new TrendingSearchHandler(monitor, queryExpander);
		customSearchHandler = new CustomSearchHandler(monitor);
		
		dyscoRequestHandler.start();
		queryExpander.start();
		
		trendingSearchHandler.start();
		customSearchHandler.start();
		
		dyscoRequestReceiver.start();
		
		this.setName("DyscoUpdateThread");
		this.start();
		
		logger.info("Set state to open");
		state = MediaSearcherState.OPEN;
		
		logger.info("Add Shutdown Hook for MediaSeacrher");
		Runtime.getRuntime().addShutdownHook(new Shutdown(this));
	}
	
	/**
	 * 
	 */
	public void run() {
		while(state == MediaSearcherState.OPEN) {
			Dysco dyscoToUpdate = queryExpander.getDyscoToUpdate();
			if(dyscoToUpdate == null) {
				try {
					synchronized(this) {
						this.wait(1000);
					}
				} catch (InterruptedException e) {
					logger.error(e);
				}
				continue;
			}
			else {
				try {
					solrdyscoHandler.insert(dyscoToUpdate);
				
					logger.info("Dysco: " + dyscoToUpdate.getId() + " is updated");
				}
				catch(Exception e) {
					logger.error(e);
				}
			}
		}
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
			
			dyscoRequestReceiver.close();
			
			if(dyscoRequestHandler != null) {
				dyscoRequestHandler.close();
				logger.info("DyscoRequestHandler is closed.");
			}
			
			trendingSearchHandler.close();
			customSearchHandler.close();
			
			state = MediaSearcherState.CLOSE;
			logger.info("MediaSearcher closed.");
		}
		catch(Exception e) {
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
		logger.info("QueryExpander " + queryExpander.getState());
		logger.info("DyscoUpdateThread " + this.getState());
		
		logger.info("#Streams: " + streams.size());
		monitor.status();
		trendingSearchHandler.status();
		queryExpander.status();
		logger.info("=================================================");
	}

	/**
	 * Class responsible for setting apart trending from custom DyScos and creating the appropriate
	 * feeds for searching them. Afterwards, it adds the DySco to the queue for further 
	 * processing from the suitable search handler. 
	 * 
	 * @author ailiakop
	 *
	 */
	private class DyscoRequestHandler extends Thread {

		private boolean isAlive = true;
		private SolrDyscoHandler solrDyscoHandler;
		
		public DyscoRequestHandler(String solrServiceUrl) {
			this.solrDyscoHandler = SolrDyscoHandler.getInstance(solrServiceUrl);
			
			this.setName("DyscoRequestHandler");
		}
		
		public void run() {
			while(isAlive) {
				Message message = dyscoRequestReceiver.getMessage();
				if(message == null) {
					try {
						synchronized(this) {
							wait(10000);	
						}
					} catch (InterruptedException e) {
						logger.error(e);
					}
					continue;
				}
				else {
					try {
						String dyscoId = message.getId();    	
						
						Action action = message.getAction();
				    	switch(action) {
				    		case NEW : 
				    			logger.info("New dysco with id: " + dyscoId);
				    		
				    			Dysco dysco = null;
				    			synchronized(solrDyscoHandler) {
				    				dysco = solrDyscoHandler.get(dyscoId);
				    			}
					    	
				    			if(dysco == null) {
				    				logger.error("Invalid dysco request: Dysco " + dyscoId + " does not exist!");
				    				continue;
				    			}

				    			trendingSearchHandler.addDysco(dysco);
				    			//customSearchHandler.addDysco(dysco);

				    			continue;
				    		case UPDATE:
				    			logger.info("Dysco with id: " + dyscoId + " needs update");
				    			//nothing to do
				    			continue;
				    		case DELETE:
				    			logger.info("Delete Dysco with id : " + dyscoId);
				    			customSearchHandler.deleteDysco(dyscoId);
				    			continue;
				    	}	
					}
					catch(Exception e) {
						logger.error(e);
					}
				}
			}
		}
		
		public void close() {
			isAlive = false;
			try {
				this.interrupt();
			}
			catch(Exception e) {
				logger.error("Failed to interrupt itself: " + e.getMessage());
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
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		/*
        DyscoDAOImpl dyscoDAO = new DyscoDAOImpl("Socialsensordb.atc.gr",
                "WebPagesDB", "WebPages", "MediaItemsDB", "MediaItems",
                "http://socialsensor.atc.gr/solr/dyscos",
                "http://socialsensor.atc.gr/solr/items",
                "http://socialsensor.atc.gr/solr/MediaItems",
                "http://socialsensor.atc.gr/solr/WebPages",
                "http://160.40.51.18:8080/VisualIndexService",
                "Prototype");
		
		SolrDyscoHandler dyscoHandler = SolrDyscoHandler.getInstance("http://socialsensor.atc.gr/solr/dyscos");
		
		Dysco dysco = dyscoHandler.findDyscoLight("618e53f9-1683-4d82-9537-4cd75c3c4337");
		System.out.println(dysco.getTitle());
		
		List<String> filters = new ArrayList<String>();
		List<String> facets = new ArrayList<String>();
		String orderBy = "publicationTime";
		Map<String, String> params = new HashMap<String, String>();
	        
		long now = System.currentTimeMillis();
		long window = 24 * 60L * 60L * 1000L;
			
		filters.add("publicationTime:[" + (now - window) + " TO " + now + "]");
		SearchEngineResponse<Item> items = dyscoDAO.findItems(dysco, filters, facets, orderBy, params, 200);
			
		SolrQueryBuilder queryBuilder = new SolrQueryBuilder();
		
		List<Query> queries = queryBuilder.getExpandedSolrQueries(items.getResults(), dysco, 10);
		
		System.out.println(queries.size());
		for(Query q : queries) {
			System.out.println(q.getName());
		}
		
		if(!filters.isEmpty())
			return;
		*/
		
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
