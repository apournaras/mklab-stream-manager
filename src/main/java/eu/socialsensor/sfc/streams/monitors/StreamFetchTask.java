package eu.socialsensor.sfc.streams.monitors;

import java.util.ArrayList;
import java.util.List;

import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.streams.Stream;
import eu.socialsensor.framework.streams.StreamException;

/**
 * Class for handling a Stream Task that is responsible for retrieving
 * multimedia content for the corresponding stream
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class StreamFetchTask implements Runnable{
	
	Stream stream;
	String streamId;
	
	int totalRetrievedItems = 0;
	
	boolean completed = false;
	boolean isSubscriber = false;
	
	List<Feed> feeds = new ArrayList<Feed>();
	
	public StreamFetchTask(Stream stream){
		this.stream = stream;
		
	}
	
	public StreamFetchTask(String streamId,Stream stream){
		this.stream = stream;
		this.streamId = streamId;
		
		if(streamId.equals("Twitter"))
			isSubscriber = true;
	}
	
	/**
	 * Adds the input feeds for the wrappers to 
	 * retrieve relevant multimedia content
	 * @param feeds
	 */
	public void addFeeds(List<Feed> feeds){
		this.feeds.addAll(feeds);
	}
	
	/**
	 * Clear the input feeds
	 */
	public void clear(){
		feeds.clear();
	}
	
	/**
	 * Returns the total number of retrieved items
	 * for the associated stream
	 * @return
	 */
	public int getTotalRetrievedItems(){
		return totalRetrievedItems;
	}
	
	/**
	 * Returns true if the stream task is completed
	 * @return
	 */
	public boolean isCompleted(){
		return completed;
	}
	
	public void restartTask(){
		completed = false;
	}
	
	/**
	 * Returns if the stream operates as a subscriber to a channel(i.e. Twitter)
	 * @return true if stream operates as a subscriber
	 */
	public boolean getIfSubscriber(){
		return isSubscriber;
	}
	
	/**
	 * Sets if the stream operates as a subscriber to a channel or not
	 * @param isSubscriber
	 */
	public void setIfSubscriber(boolean isSubscriber){
		this.isSubscriber = isSubscriber;
	}
	
	/**
	 * Retrieves images/videos using the feeds
	 * as input to the corresponding wrapper to the stream
	 */
	@Override
	public void run(){
		try {
			if(!isSubscriber)
				totalRetrievedItems = stream.search(feeds);
			else
				stream.search(feeds);
		} catch (StreamException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		completed = true;
	}
}
