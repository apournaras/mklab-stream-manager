package eu.socialsensor.sfc.streams.monitors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
	Collection<Stream> streams;
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
	public void addStreams(Collection<Stream> streams){
		this.streams = streams;
		
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
	 * @param feeds
	 */
	public void start(List<Feed> feeds){
		totalRetrievedItems = 0;
		
		if(feeds.isEmpty() || feeds == null)
			return;
		
		for(Stream stream : streams){
			
			StreamFetchTask streamTask = new StreamFetchTask(stream);
			streamTask.addFeeds(feeds);
			streamsFetchTasks.add(streamTask);
			executor.execute(streamTask);
			
		}
	}
	/**
	 * Checks if all streams are finished retrieving items
	 * and if yes sets the stream monitor as finished
	 * @return
	 */
	public boolean isMonitorFinished(){
		int allStreamsDone = 0;
		List<StreamFetchTask> finishedTasks = new ArrayList<StreamFetchTask>();
		while(allStreamsDone < streamsFetchTasks.size())
			for(StreamFetchTask streamTask : streamsFetchTasks){
				if(streamTask.completed && !finishedTasks.contains(streamTask)){
					totalRetrievedItems += streamTask.getTotalRetrievedItems();
					finishedTasks.add(streamTask);
					allStreamsDone++;
				}
					
			}
		//System.out.println("Streams monitor is done!");
		streamsFetchTasks.clear();
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
