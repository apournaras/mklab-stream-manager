package eu.socialsensor.sfc.streams.search;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.dysco.Dysco;
import eu.socialsensor.sfc.input.DataInputType;
import eu.socialsensor.sfc.input.FeedsCreator;
import eu.socialsensor.sfc.streams.monitors.StreamsMonitor;

/**
	 * Class responsible for Custom DySco requests 
	 * Custom DyScos are searched periodically in the system in a two-day period of time. 
	 * They are deleted in case the user deletes them or the searching period has expired. 
	 * The frequency Custom DyScos are searched in the system has been set in 15 minutes.
	 * @author ailiakop
	 *
	 */
	public class CustomSearchHandler extends SearchHandler {
		
		private Map<String, Dysco> dyscos = Collections.synchronizedMap(new HashMap<String, Dysco>());
		private Map<String, Long> dyscosLifetime = Collections.synchronizedMap(new HashMap<String, Long>());
		private Map<String, Long> dyscosTimestamps = Collections.synchronizedMap(new HashMap<String, Long>());
		
		private static final long frequency = 15 * 60 * 1000; 			//fifteen minutes
		private static final long periodOfTime = 2 * 24 * 3600 * 1000; 	//two days
		
		public CustomSearchHandler(StreamsMonitor monitor) {
			super(monitor);
			
			logger = Logger.getLogger(CustomSearchHandler.class);
		}
		
		public void addCustomDysco(Dysco dysco, List<Feed> inputFeeds) {
			String dyscoId = dysco.getId();
			logger.info("New incoming Custom DySco: " + dyscoId + " with " + inputFeeds.size() + " searchable feeds");
			try {
				long currentTimestamp = System.currentTimeMillis();
				
				dyscosLifetime.put(dyscoId, currentTimestamp);
				dyscosTimestamps.put(dyscoId, currentTimestamp);
				dyscos.put(dyscoId, dysco);
				
				super.addDysco(dysco);
				
			}
			catch(Exception e) {
				logger.error(e);
				deleteDysco(dyscoId);
			}
		}
	
		/**
		 * Updates the queue of custom DyScos and re-examines or deletes 
		 * them according to their time in the system
		 */
		protected synchronized void update() {

			long currentTime = System.currentTimeMillis();
			List<String> dyscosToDelete = new ArrayList<String>();
			
			for(Entry<String, Long> entry : dyscosTimestamps.entrySet()) {
				String dyscoId = entry.getKey();
				Long lastSearchTime = entry.getValue();
				if(currentTime - lastSearchTime > frequency) {
					
					logger.info("Custom DySco " +  dyscoId + "  frequency: " + frequency + " currentTime: " + currentTime + 
							" dysco's last search time: " + lastSearchTime);
					
					if(currentTime - dyscosLifetime.get(dyscoId) > periodOfTime) {
						logger.info("periodOfTime: " + periodOfTime + " currentTime: " + currentTime + " dysco's lifetime: " + dyscosLifetime.get(dyscoId));
						logger.info("Remove Custom DySco " + dyscoId + " from the queue - expired");
						dyscosToDelete.add(dyscoId);
						continue;
					}
					
					logger.info("Add Custom DySco " + dyscoId + " again in the queue for searching");
					Dysco dysco = dyscos.get(dyscoId);
					if(dysco != null) {
						dyscosQueue.add(dysco);
						dyscosTimestamps.put(dyscoId, currentTime);	
					}
				}
			}
			
			if(!dyscosToDelete.isEmpty()) {
				for(String dyscoToDelete : dyscosToDelete) {
					deleteDysco(dyscoToDelete);
				}
			}
		}
		
		@Override
		protected void searchForDysco(Dysco dysco) {
			String dyscoId = dysco.getId();
			logger.info("Custom Media Searcher handling: " + dyscoId);
			try {
				//List<Feed> feeds = inputFeedsPerDysco.get(dyscoId);
				FeedsCreator feedsCreator = new FeedsCreator(DataInputType.DYSCO, dysco);
				List<Feed> feeds = feedsCreator.getQuery();
				
				List<Item> customDyscoItems = search(feeds);
				logger.info("Total Items retrieved for Custom DySco " + dyscoId + " : " + customDyscoItems.size());
			}
			catch(Exception e) {
				logger.error(e);
			}
		}

		@Override
		public void deleteDysco(String dyscoId) {
			dyscosLifetime.remove(dyscoId);
			dyscosTimestamps.remove(dyscoId);
			Dysco dysco = dyscos.remove(dyscoId);
			
			dyscosQueue.remove(dysco);
		}
		
	}