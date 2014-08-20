package eu.socialsensor.sfc.streams.search;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.common.domain.Item;

/**
	 * Class responsible for Custom DySco requests 
	 * Custom DyScos are searched periodically in the system in a two-day period of time. 
	 * They are deleted in case the user deletes them or the searching period has expired. 
	 * The frequency Custom DyScos are searched in the system has been set in 10 minutes.
	 * @author ailiakop
	 *
	 */
	public class CustomSearchHandler extends Thread {
		
		public final Logger logger = Logger.getLogger(CustomSearchHandler.class);
		
		private BlockingQueue<String> customDyscoQueue = new LinkedBlockingQueue<String>();
		
		private Map<String, List<Feed>> inputFeedsPerDysco = Collections.synchronizedMap(new HashMap<String, List<Feed>>());
		private Map<String, Long> requestsLifetime = Collections.synchronizedMap(new HashMap<String, Long>());
		private Map<String, Long> requestsTimestamps = Collections.synchronizedMap(new HashMap<String, Long>());
		
		private MediaSearcher searcher;

		private boolean isAlive = true;

		private Queue<String> requestsToDelete;
		
		private static final long frequency = 10 * 60 * 1000; 			//ten minutes
		private static final long periodOfTime = 2 * 24 * 3600 * 1000; 	//two days
		
		public CustomSearchHandler(MediaSearcher mediaSearcher, Queue<String> requestsToDelete) {
			this.searcher = mediaSearcher;
			this.requestsToDelete = requestsToDelete;
		}
		
		public void addCustomDysco(String dyscoId, List<Feed> inputFeeds) {
			logger.info("New incoming Custom DySco: " + dyscoId + " with " + inputFeeds.size() + " searchable feeds");
			try {
				customDyscoQueue.add(dyscoId);
				inputFeedsPerDysco.put(dyscoId, inputFeeds);
				
				long timestamp = System.currentTimeMillis();
				requestsLifetime.put(dyscoId, timestamp);
				requestsTimestamps.put(dyscoId, timestamp);
			}
			catch(Exception e) {
				logger.error(e);
				customDyscoQueue.remove(dyscoId);
				deleteCustomDysco(dyscoId);
			}
		}
		
		public void deleteCustomDysco(String dyscoId) {
			inputFeedsPerDysco.remove(dyscoId);
			requestsLifetime.remove(dyscoId);
			requestsTimestamps.remove(dyscoId);
		}
		
		public void run() {
			String dyscoId = null;
			while(isAlive) {
				try {
					updateCustomQueue();
				}
				catch(Exception e) {
					logger.error("Failed to update Custom Dyscos Queue.", e);
				}
				
				dyscoId = poll();
				if(dyscoId == null) {
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
					logger.info("Media Searcher handling #" + dyscoId);
					try {
						List<Feed> feeds = inputFeedsPerDysco.get(dyscoId);
						List<Item> customDyscoItems = searcher.search(feeds);
						logger.info("Total Items retrieved for Custom DySco " + dyscoId + " : " + customDyscoItems.size());
						customDyscoItems.clear();
					}
					catch(Exception e) {
						logger.error(e);
					}
				}
			}
		}
		
		/**
		 * Polls a custom DySco request from the queue to search
		 * @return
		 */
		private String poll() {
			String request = null;
			try {
				request = customDyscoQueue.take();
			} catch (InterruptedException e) {

			}
			return request;
		}
		
		/**
		 * Stops Custom Search Handler
		 */
		public synchronized void close() {
			isAlive = false;
			try {
				this.interrupt();
			}
			catch(Exception e) {
				logger.error("Failed to interrupt itself", e);
			}
		}
	
		/**
		 * Updates the queue of custom DyScos and re-examines or deletes 
		 * them according to their time in the system
		 */
		private synchronized void updateCustomQueue() {
			
			List<String> requestsToRemove = new ArrayList<String>();
			long currentTime = System.currentTimeMillis();
			
			for(Map.Entry<String, Long> entry : requestsLifetime.entrySet()) {
				String key = entry.getKey();
				Long value = entry.getValue();
				if(currentTime - value > frequency) {
					
					logger.info("Custom DySco " +  key + "  frequency: " + frequency + " currentTime: " + currentTime + 
							" dysco's last search time: " + value);
					
					if(currentTime - requestsTimestamps.get(key)> periodOfTime) {
						logger.info("periodOfTime: " + periodOfTime + " currentTime: " + currentTime + " dysco's lifetime: " + requestsTimestamps.get(key));
						logger.info("Remove Custom DySco " + key + " from the queue - expired");
						requestsToRemove.add(key);
						continue;
					}
					
					logger.info("Add Custom DySco " + key + " again in the queue for searching");
					customDyscoQueue.add(key);
					requestsLifetime.put(key, System.currentTimeMillis());	
				}
			}
			
			if(!requestsToRemove.isEmpty() || !requestsToDelete.isEmpty()) {
				for(String requestToRemove : requestsToRemove) {
					deleteCustomDysco(requestToRemove);
				}
				for(String requestToDelete : requestsToDelete) {
					deleteCustomDysco(requestToDelete);
				}
				requestsToRemove.clear();	
				requestsToDelete.clear();
			}
		}
	}