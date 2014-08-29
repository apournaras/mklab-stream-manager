package eu.socialsensor.sfc.streams.search;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.dysco.Dysco;
import eu.socialsensor.sfc.streams.monitors.StreamsMonitor;

public abstract class SearchHandler extends Thread {

	protected Logger logger = Logger.getLogger(SearchHandler.class);
	protected StreamsMonitor monitor = null;
	
	private boolean isAlive = true;
	private long totalRetrievedItems = 0;
	
	protected BlockingQueue<Dysco> dyscosQueue = new LinkedBlockingDeque<Dysco>();
	protected Map<String, List<Feed>> inputFeedsPerDysco = Collections.synchronizedMap(new HashMap<String, List<Feed>>());
	//protected Map<String, List<Feed>> inputFeedsPerDysco = new ConcurrentHashMap<String, List<Feed>>();
	
	public SearchHandler(StreamsMonitor monitor) {
		this.monitor = monitor;
	}
	
	/**
	 * Searches in all social media defined in the configuration file
	 * for the list of feeds that is given as input and returns the retrieved items
	 * @param feeds
	 * @param streamsToSearch
	 * @return the list of the items retrieved
	 */
	public List<Item> search(List<Feed> feeds) {
		List<Item> items = new ArrayList<Item>();
		
		if(feeds != null && !feeds.isEmpty()) {
			try {
				synchronized(monitor) {	
					monitor.retrieve(feeds);	
					while(!monitor.areAllStreamsFinished()) {
						try {
							Thread.sleep(500);
						} catch (InterruptedException e) {
							logger.error(e);
						}
					}		
					
					items.addAll(monitor.getTotalRetrievedItems());
					totalRetrievedItems += items.size();
				}
			} catch (Exception e) {
				logger.error(e);
			}
		}
		return items;
	}
	
	public void addDysco(Dysco dysco, List<Feed> inputFeeds) {
		try {
			logger.info("New incoming Trending DySco: " + dysco.getId() + " with " + inputFeeds.size() + " searchable feeds");
			dyscosQueue.put(dysco);
			inputFeedsPerDysco.put(dysco.getId(), inputFeeds);
			logger.info("Putted in dyscos queue (" + dyscosQueue.size() + ")");
		}
		catch(Exception e) {
			logger.error(e);
		}
	}
	
	public abstract void deleteDysco(String dyscoId);
	
	public void run() {
		Dysco dysco = null;
		while(isAlive) {
			
			update();
			 
			dysco = poll();
			if(dysco == null) {
				try {
					synchronized(this) {
						this.wait(1000);
					}
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
				continue;
			}
			else {
				try {
					searchForDysco(dysco);
				}
				catch(Exception e) {
					logger.error("Error during searching for dysco: " + dysco.getId() + " of type: " + dysco.getDyscoType());
					logger.error("Exception: " + e);
				}
			}
		}
	}
	
	/**
	 * Stops SearchHandler
	 */
	public synchronized void close() {
		isAlive = false;
		try {
			this.interrupt();
		}
		catch(Exception e) {
			logger.error("Failed to interrupt itself: " + e.getMessage());
		}
	}
	
	/**
	 * Polls a DySco request from the queue to search
	 * @return Dysco
	 */
	/**
	 * Polls a trending DySco request from the queue to search
	 * @return
	 */
	protected Dysco poll() {
		return dyscosQueue.poll();
	}
	
	public void status() {
		logger.info("DyscoQueue:" + dyscosQueue.size());
		logger.info("inputFeedsPerDysco:" + inputFeedsPerDysco.size());
		logger.info("totalRetrievedItems:" + totalRetrievedItems);
	}
	
	protected abstract void searchForDysco(Dysco dysco);
	
	protected abstract void update();
		
}
