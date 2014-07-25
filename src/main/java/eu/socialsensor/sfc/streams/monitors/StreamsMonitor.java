package eu.socialsensor.sfc.streams.monitors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.streams.Stream;

/**
 * Thread-safe class for monitoring the streams that correspond to each social network
 * Currently 7 social networks are supported (Twitter, Youtube,Flickr,Instagram,Tumblr,Facebook,GooglePlus)
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class StreamsMonitor {
	
	private static final long DEFAULT_REQUEST_TIME = 5 * 60 * 60000; 
	
	public final Logger logger = Logger.getLogger(StreamsMonitor.class);

	private ExecutorService executor;
	
	private Map<String, Stream> streams = new HashMap<String,Stream>();
	private Map<String, List<Feed>> feedsPerStream = new HashMap<String,List<Feed>>();
	private Map<String, Long> requestTimePerStream = new HashMap<String,Long>();
	private Map<String, Long> runningTimePerStream = new HashMap<String,Long>();
	private Map<String, StreamFetchTask> streamsFetchTasks = new HashMap<String,StreamFetchTask>();
	
	private List<Item> totalRetrievedItems = new ArrayList<Item>();
	
	boolean isFinished = false;
	
	private ReInitializer reInitializer = new ReInitializer();
	
	public StreamsMonitor(int numberOfStreams) {
		executor = Executors.newFixedThreadPool(numberOfStreams);
	}
	
	public List<Item> getTotalRetrievedItems() {
		return totalRetrievedItems;
	}
	
	public int getNumberOfStreamFetchTasks() {
		return streamsFetchTasks.size();
	}
	
	/**
	 * Adds the streams to the monitor
	 * @param streams
	 */
	public void addStreams(Map<String,Stream> streams) {
		for(String streamId : streams.keySet()) {
			addStream(streamId,streams.get(streamId));
		}
	}

	/**
	 * Add one stream to the monitor mapped to its id
	 * @param streamId
	 * @param stream
	 */
	public void addStream(String streamId,Stream stream) {
		this.streams.put(streamId, stream);
		this.requestTimePerStream.put(streamId, DEFAULT_REQUEST_TIME);
	}
	
	/**
	 * Adds a stream to the monitor mapped to its id and the feeds that
	 * will be used to retrieve relevant content from the aforementioned
	 * stream. Request time refers to the time period the stream will 
	 * serve search requests. 
	 * @param streamId
	 * @param stream
	 * @param feeds
	 */
	public void addStream(String streamId,Stream stream,List<Feed> feeds) {
		this.streams.put(streamId, stream);
		this.feedsPerStream.put(streamId, feeds);
		this.requestTimePerStream.put(streamId, DEFAULT_REQUEST_TIME);
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
	
	public Stream getStream(String streamId) {
		return streams.get(streamId);
	}
	
	public void setStreamRequestTime(String streamId,Long requestTime) {
		this.requestTimePerStream.put(streamId, requestTime);
	}
	/**
	 * Starts searching into the specific stream by assigning its feeds to stream fetch tasks and
	 * executing them.
	 * @param streamId
	 */
	public void startStream(String streamId) {
		if(!streams.containsKey(streamId)) {
			logger.error("Stream "+streamId+" needs to be added to the monitor first");
			return;
		}
		
		StreamFetchTask streamTask = null;
		try {
			streamTask = new StreamFetchTask(streams.get(streamId),feedsPerStream.get(streamId));
		} catch (Exception e) {
			logger.error(e);
		}
		streamsFetchTasks.put(streamId, streamTask);
		executor.execute(streamTask);
		runningTimePerStream.put(streamId, System.currentTimeMillis());
		
		logger.info("Start stream task : "+streamId+" with "+feedsPerStream.get(streamId).size()+" feeds");
	}

	/**
	 * Starts the retrieval process for all streams. Each stream is served
	 * by a different thread->StreamFetchTask
	 * @param 
	 */
	public void startAllStreamsAtOnce() {
		for(Map.Entry<String, Stream> entry : streams.entrySet()){
			startStream(entry.getKey());
		}
		reInitializer.start();
	}
	
	public void startReInitializer() {
		reInitializer.start();
	}
	
	/**
	 *	Starts the retrieval process for all the streams added. Each stream is served by a different thread->StreamFetchTask. 
	 *	All streams are assigned the same input feeds for retrieving relevant content.
	 * 	@param feeds
	 * 	@throws Exception 
	 */
	public void retrieveFromAllStreams(List<Feed> feeds) throws Exception {
		
		totalRetrievedItems.clear();
		for(Map.Entry<String, Stream> entry : streams.entrySet()) {
			
			StreamFetchTask streamTask = new StreamFetchTask(entry.getValue(), feeds);
			streamsFetchTasks.put(entry.getKey(), streamTask);
			executor.execute(streamTask);
			runningTimePerStream.put(entry.getKey(), System.currentTimeMillis());
			
			System.out.println("Start stream task : " + entry.getKey() + " with " + feeds.size() + " feeds");
		}
	}
	
	/**
	 * Starts the retrieval process for all the streams specified here with a 
	 * standard list of input feeds to retrieve relevant content. Each stream is served
	 * by a different thread->StreamFetchTask.
	 * @param selectedStreams
	 * @param feeds
	 */
	public void retrieveFromSelectedStreams(Set<String> selectedStreams, List<Feed> feeds) {
		totalRetrievedItems.clear();
		
		for(Map.Entry<String, Stream> entry : streams.entrySet()) {
			if(selectedStreams.contains(entry.getKey())) {
				StreamFetchTask streamTask = null;
				try {
					streamTask = new StreamFetchTask(entry.getValue(), feeds);
				} catch (Exception e) {
					logger.error(e);
				}
				
				streamsFetchTasks.put(entry.getKey(), streamTask);
				executor.execute(streamTask);
				runningTimePerStream.put(entry.getKey(), System.currentTimeMillis());
				
				logger.info("Start stream task : " + entry.getKey() + " with " + feeds.size() + " feeds");
			}
		}
	}
	
	/**
	 * Restarts a stream to start retrieving again for relevant content to its input feeds. 
	 * Reinitializer checks the last time the stream was searched and if the specified time 
	 * period has passed, restarts the stream. 
	 * @author ailiakop
	 *
	 */
	private class ReInitializer extends Thread {
		
		private Map<String,Long> reformedRunningTimes = new HashMap<String,Long>();
		
		public ReInitializer() {
			logger.info("ReInitializer Thread instantiated");
		}
		
		public void run() {
			logger.info("ReInitializer Thread started");
			while(!isFinished) {
				long currentTime = System.currentTimeMillis();
				
				for(String streamId : runningTimePerStream.keySet()) {
					if((currentTime - runningTimePerStream.get(streamId)) >= requestTimePerStream.get(streamId)) {
						if(streamsFetchTasks.get(streamId).isCompleted()) {
							streamsFetchTasks.get(streamId).restartTask();
							executor.execute(streamsFetchTasks.get(streamId));
							reformedRunningTimes.put(streamId, System.currentTimeMillis());
						}
					}
				}
				
				for(String streamId : reformedRunningTimes.keySet()) {
					logger.info("Reinitializing Stream: " + streamId);
					runningTimePerStream.put(streamId, reformedRunningTimes.get(streamId));
				}
				
				reformedRunningTimes.clear();
				
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					logger.error(e);
				}
			}
		}
		
	}
	
	/**
	 * Checks if all streams are finished retrieving items
	 * and if so sets returns true
	 * @return
	 */
	public boolean areAllStreamsFinished() {
		int allStreamsDone = 0;
		int allRunningStreams;
		
		List<StreamFetchTask> finishedTasks = new ArrayList<StreamFetchTask>();
		
		allRunningStreams = streamsFetchTasks.size();
		
		while(allStreamsDone < allRunningStreams) {
			for(StreamFetchTask streamTask : streamsFetchTasks.values()) {
				if(streamTask.isCompleted() && !finishedTasks.contains(streamTask)) {
					totalRetrievedItems.addAll(streamTask.getTotalRetrievedItems());
					finishedTasks.add(streamTask);
					allStreamsDone++;
				}	
			}
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		streamsFetchTasks.clear();
		finishedTasks.clear();
		
		return true;
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
}