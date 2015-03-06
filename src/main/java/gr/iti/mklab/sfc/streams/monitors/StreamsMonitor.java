package gr.iti.mklab.sfc.streams.monitors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.common.domain.feeds.Feed;
import gr.iti.mklab.sfc.streams.Stream;


/**
 * Thread-safe class for monitoring the streams that correspond to each social network
 * Currently 7 social networks are supported (Twitter, Youtube, Flickr, Instagram, Tumblr, Facebook, GooglePlus)
 * 
 * @author Manos Schinas
 * @email  manosetro@iti.gr
 */
public class StreamsMonitor implements Runnable {
	
	// 30 minutes
	private static final long DEFAULT_REQUEST_PERIOD = 1 * 15 * 60000; 
	
	public final Logger logger = Logger.getLogger(StreamsMonitor.class);

	private ExecutorService executor;
	
	private Map<String, Stream> streams = new HashMap<String, Stream>();
	private Map<String, Set<Feed>> feedsPerStream = new HashMap<String, Set<Feed>>();
	
	private Map<String, StreamFetchTask> streamsFetchTasks = new HashMap<String, StreamFetchTask>();
	
	boolean isFinished = false;
	
	public StreamsMonitor(int numberOfStreams) {
		logger.info("Initialize Execution Service with " + numberOfStreams + " threads.");
		executor = Executors.newFixedThreadPool(numberOfStreams + 1);
	}
	
	public int getNumberOfStreamFetchTasks() {
		return streamsFetchTasks.size();
	}
	
	/**
	 * Adds the streams to the monitor
	 * @param streams
	 */
	public void addStreams(Map<String, Stream> streams) {
		for(Entry<String, Stream> streamEntry : streams.entrySet()) {
			addStream(streamEntry.getKey(), streamEntry.getValue());
		}
	}

	/**
	 * Add one stream to the monitor mapped to its id
	 * @param streamId
	 * @param stream
	 */
	public void addStream(String streamId, Stream stream) {
		this.streams.put(streamId, stream);
	}
	
	/**
	 * Adds a stream to the monitor mapped to its id and the feeds that
	 * will be used to retrieve relevant content from the aforementioned
	 * stream. Request time refers to the time period the stream will 
	 * serve search requests. 
	 * 
	 * @param streamId
	 * @param stream
	 * @param feeds
	 */
	public void addStream(String streamId, Stream stream, Set<Feed> feeds) {
		this.streams.put(streamId, stream);
		this.feedsPerStream.put(streamId, feeds);
	}
	
	public Stream getStream(String streamId) {
		return streams.get(streamId);
	}
	
	/**
	 * Adds a feed to the monitor
	 * @param stream
	 */
	public void addFeed(String streamId, Feed feed) {
		StreamFetchTask fetchTask = streamsFetchTasks.get(streamId);
		if(fetchTask != null) {
			fetchTask.addFeed(feed);
		}
	}
	
	/**
	 * Adds a feed to the monitor
	 * @param stream
	 */
	public void removeFeed(String streamId, Feed feed) {
		StreamFetchTask fetchTask = streamsFetchTasks.get(streamId);
		if(fetchTask != null) {
			fetchTask.removeFeed(feed);
		}
	}
	
	/**
	 * Adds a feed to the monitor
	 * @param stream
	 */
	public void addFeeds(String streamId, List<Feed> feeds) {
		StreamFetchTask fetchTask = streamsFetchTasks.get(streamId);
		if(fetchTask != null) {
			fetchTask.addFeeds(feeds);
		}
	}
	
	/**
	 * Adds a feed to the monitor
	 * @param stream
	 */
	public void addFeeds(List<Feed> feeds) {
		for(StreamFetchTask fetchTask : streamsFetchTasks.values()) {
			fetchTask.addFeeds(feeds);
		}
	}
	
	/**
	 * Starts searching into the specific stream by assigning its feeds to stream fetch tasks and
	 * executing them.
	 * @param streamId
	 */
	private void startStream(String streamId) {
		
		if(!streams.containsKey(streamId)) {
			logger.error("Stream " + streamId + " needs to be added to the monitor first");
			return;
		}
		
		try {			
			logger.info("Start " + streamId + " Fetch Task");
			StreamFetchTask streamTask = new StreamFetchTask(streams.get(streamId));
			streamsFetchTasks.put(streamId, streamTask);
		} catch (Exception e) {
			logger.error(e);
		}
	}

	/**
	 * Starts the retrieval process for all streams. Each stream is served
	 * by a different thread->StreamFetchTask
	 * @param 
	 */
	public void startStreams() {
		for(String streamId : streams.keySet()) {
			startStream(streamId);
		}
		executor.submit(this);
	}
	
	/**
	 * Stops the monitor - waits for all streams to shutdown
	 */
	public void stop() {
		isFinished = true;
		executor.shutdown();
		
        while (!executor.isTerminated()) {
        	try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				logger.error(e);
			}
        	logger.info("Waiting for StreamsMonitor to shutdown");
        }
        logger.info("Streams Monitor stopped");
	}

	@Override
	public void run() {
		Map<String, Future<Integer>> responses = new HashMap<String, Future<Integer>>();
		while(!isFinished) {
			
			for(String streamId : streamsFetchTasks.keySet()) {
				StreamFetchTask task = streamsFetchTasks.get(streamId);
				
				if((System.currentTimeMillis() - task.getLastRuntime()) < DEFAULT_REQUEST_PERIOD) {
					continue;
				}
				
				Future<Integer> response = responses.get(streamId);
				if(response == null || response.isDone()) {
					Future<Integer> future = executor.submit(task);
					responses.put(streamId, future);
				}
			}
			
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				return;
			}
		}
	}
	
}