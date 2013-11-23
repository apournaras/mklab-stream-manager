package eu.socialsensor.sfc.streams.monitors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.streams.Stream;

/**
 * Class for monitoring the streams that correspond to each social network
 * (Twitter, Youtube,Flickr,Instagram,Tumblr,Facebook,GooglePlus)
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class StreamsMonitor {

	ExecutorService executor;
	
	Map<String,Stream> streams = new HashMap<String,Stream>();
	Map<String,List<Feed>> feedsPerStream = new HashMap<String,List<Feed>>();
	
	List<StreamFetchTask> streamsFetchTasks = new ArrayList<StreamFetchTask>();
	
	int totalRetrievedItems = 0 ;
	
	boolean isFinished = false;
	
	
	public StreamsMonitor(int numberOfStreams){
		executor = Executors.newFixedThreadPool(numberOfStreams);
	}
	
	/**
	 * Adds the streams to the monitor
	 * @param streams
	 */
	public void addStreams(Map<String,Stream> streams){
		this.streams = streams;
		
	}
	
	/**
	 * Adds a stream to the monitor
	 * @param stream
	 */
	public void addStream(String streamId,Stream stream,List<Feed> feeds){
		this.streams.put(streamId, stream);
		this.feedsPerStream.put(streamId, feeds);
	}
	
	/**
	 * Returns the total number of the items that were retrieved with
	 * the wrappers of the streams
	 * @return
	 */
	public int getTotalRetrievedItems(){
		return totalRetrievedItems;
	}

	/**
	 * Starts the retrieval process for each stream separately 
	 * as a different thread
	 * @param 
	 */
	public void start(){
		totalRetrievedItems = 0;
		
		for(Map.Entry<String, Stream> entry : streams.entrySet()){
			System.out.println("Start stream task : "+entry.getKey()+" with "+feedsPerStream.get(entry.getKey()).size()+" feeds");
			StreamFetchTask streamTask = new StreamFetchTask(entry.getKey(),entry.getValue());
			streamTask.addFeeds(feedsPerStream.get(entry.getKey()));
			streamsFetchTasks.add(streamTask);
			executor.execute(streamTask);
			
		}
	}
	
	/**
	 * Starts the retrieval process for each stream separately 
	 * as a different thread with the same input feeds
	 * @param feeds
	 */
	public void start(List<Feed> feeds){
		totalRetrievedItems = 0;
		
		for(Map.Entry<String, Stream> entry : streams.entrySet()){
			
			StreamFetchTask streamTask = new StreamFetchTask(entry.getKey(),entry.getValue());
			streamTask.addFeeds(feeds);
			streamsFetchTasks.add(streamTask);
			executor.execute(streamTask);
			
		}
	}
	
	public void reinitializePolling(){
		while(!areAllStreamFinished()){
			System.out.println("Stream Monitor - Wait for all streams to finish - normally should not happen");
		}
		
		for(StreamFetchTask streamTask : streamsFetchTasks){
			if(!streamTask.isSubscriber){
				streamTask.restartTask();
				executor.execute(streamTask);
			}
		}
		
		System.out.println("Streams Monitor reinitialized");
	}
	/**
	 * Checks if all streams are finished retrieving items
	 * and if yes sets the stream monitor as finished
	 * @return
	 */
	public boolean areAllStreamFinished(){
		int allStreamsDone = 0;
		int allRunningPollingStreams;
		
		List<StreamFetchTask> finishedTasks = new ArrayList<StreamFetchTask>();
		
		//include only polling streams
		if(streams.keySet().contains("Twitter"))
			allRunningPollingStreams = streamsFetchTasks.size() - 1;
		else
			allRunningPollingStreams = streamsFetchTasks.size();
		
		while(allStreamsDone < allRunningPollingStreams)
			for(StreamFetchTask streamTask : streamsFetchTasks){
				if(!streamTask.getIfSubscriber() && streamTask.completed && !finishedTasks.contains(streamTask)){
					totalRetrievedItems += streamTask.getTotalRetrievedItems();
					finishedTasks.add(streamTask);
					allStreamsDone++;
				}
					
			}
		
		return true;
	}
	
	/**
	 * Stops the monitor - waits for all streams to shutdown
	 */
	public void stop(){
		executor.shutdown();
        while (!executor.isTerminated()) {
        	System.out.println("Waiting for StreamsMonitor to shutdown");
        }
        System.out.println("Streams Monitor stopped");
	}
}
