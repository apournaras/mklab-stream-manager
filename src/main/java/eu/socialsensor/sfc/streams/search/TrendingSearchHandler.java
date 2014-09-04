package eu.socialsensor.sfc.streams.search;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.dysco.Dysco;
import eu.socialsensor.sfc.input.DataInputType;
import eu.socialsensor.sfc.input.FeedsCreator;
import eu.socialsensor.sfc.streams.monitors.StreamsMonitor;

/**
	 * Class responsible for Trending DySco requests 
	 * It performs custom search to retrieve an item collection
	 * based on Dysco's primary queries and afterwards applies query
	 * expansion via the Query Builder module to extend the search
	 * and detect more relative content to the DySco. 
	 * 
	 * @author ailiakop
	 */
	public class TrendingSearchHandler extends SearchHandler {
		
		public final Logger logger = Logger.getLogger(TrendingSearchHandler.class);
		
		private Set<String> targetedStreams = new HashSet<String>();

		private QueryExpander queryExpander;
		
		public TrendingSearchHandler(StreamsMonitor monitor, QueryExpander queryExpander) {
			super(monitor);
			this.queryExpander = queryExpander;
			
			targetedStreams.add("Facebook");
			targetedStreams.add("Flickr");
			targetedStreams.add("Instagram");
		}
		
		/**
		 * Searches for a Trending DySco at two stages. At the first stage the algorithm 
		 * retrieves content from social media using DySco's primal queries, whereas 
		 * at the second stage it uses the retrieved content to expand the queries.
		 * The final queries, which are the result of merging the primal and the expanded
		 * queries are used to search further in social media for additional content. 
		 * @param dysco
		 * 
		 */
		protected void searchForDysco(Dysco dysco) {
			
			String dyscoId = dysco.getId();
			
			long start = System.currentTimeMillis();
			logger.info("Trending Media Searcher handling: " + dyscoId);
			
			//first search
			FeedsCreator feedsCreator = new FeedsCreator(DataInputType.DYSCO, dysco);
			List<Feed> feeds = feedsCreator.getQuery();
			
			List<Feed> simpleFeeds = getSimpleFeeds(dysco);
			search(simpleFeeds, targetedStreams);
			
			logger.info(feeds.size() + " feeds for " + dyscoId); 
			logger.info(simpleFeeds.size() + " simple feeds for " + dyscoId);

			List<Item> retrievedItems = search(feeds);
			
			long t1 = System.currentTimeMillis();
			logger.info("Time for First Search for Trending DySco: " + dyscoId + " is " + (t1-start)/1000 + " sec.");
			logger.info(retrievedItems.size() + " items retrieved from " + feeds.size() + 
					" feeds, for Trending DySco " + dyscoId );
			
			queryExpander.addDysco(dysco, retrievedItems);
			
			long end = System.currentTimeMillis();
			logger.info("Total Time searching for Trending DySco: " + dyscoId + " is " + (end-start)/1000 + " sec.");
		}

		
		@Override
		protected void update() {
			// Nothing to update in Trending Dyscos
		}

		@Override
		public void deleteDysco(String dyscoId) {
			// Nothing to delete in Trending Dyscos
		}

	}