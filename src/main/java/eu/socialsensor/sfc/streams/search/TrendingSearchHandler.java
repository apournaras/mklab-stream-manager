package eu.socialsensor.sfc.streams.search;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.UUID;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.Keyword;
import eu.socialsensor.framework.common.domain.Query;
import eu.socialsensor.framework.common.domain.dysco.Dysco;
import eu.socialsensor.framework.common.domain.feeds.KeywordsFeed;
import eu.socialsensor.sfc.builder.SolrQueryBuilder;
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
		
		private SolrQueryBuilder queryBuilder;

		private long totalRetrievedItems = 0;

		private Queue<Dysco> dyscosToUpdate; 

		public TrendingSearchHandler(StreamsMonitor monitor, Queue<Dysco> dyscosToUpdate) {
			super(monitor);
			
			this.dyscosToUpdate = dyscosToUpdate;
			
			try {
				this.queryBuilder = new SolrQueryBuilder();
			} catch (Exception e) {
				logger.error(e);
			}
			
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
			List<Feed> primalFeeds = inputFeedsPerDysco.remove(dyscoId);
			
			List<Item> retrievedItems = search(primalFeeds);
			
			totalRetrievedItems += retrievedItems.size();
			
			long t1 = System.currentTimeMillis();
			logger.info("Time for First Search for Trending DySco: " + dyscoId + " is " + (t1-start)/1000 + " sec.");
			logger.info("Items retrieved for Trending DySco : " + dyscoId + " : " + retrievedItems.size());
			
			long t2 = System.currentTimeMillis();
			
			// Second search
			// Expand Queries
			List<Query> queries = queryBuilder.getExpandedSolrQueries(retrievedItems, dysco, 5);
			List<Query> expandedQueries = new ArrayList<Query>();
			for(Query q : queries) {
				if(q.getIsFromExpansion()) {
					expandedQueries.add(q);
				}
			}
			
			logger.info("Number of additional queries for Trending DySco: " + dyscoId + " is " + expandedQueries.size());
			
			long t3 = System.currentTimeMillis();
			logger.info("Time for computing queries for Trending DySco: " + dyscoId + " is " + (t3-t2)/1000 + " sec.");
			
			if(!expandedQueries.isEmpty()) {
				dysco.setSolrQueries(queries);
				dyscosToUpdate.add(dysco);
				
				Date sinceDate = new Date(System.currentTimeMillis() - 2 * 24 * 3600 * 1000);
				List<Feed> newFeeds = transformQueriesToKeywordsFeeds(expandedQueries, sinceDate);
				List<Item> secondSearchItems = search(newFeeds);
				
				long t4 = System.currentTimeMillis();
				logger.info("Total Items retrieved for Trending DySco : " + dyscoId + " : " + secondSearchItems.size());
				logger.info("Time for Second Search for Trending DySco: " + dyscoId + " is " + (t4-t3)/1000 + " sec.");
			}
			else {
				System.out.println("No queries to update");
			}
			
			long end = System.currentTimeMillis();
			logger.info("Total Time searching for Trending DySco: " + dyscoId + " is " + (end-start)/1000 + " sec.");
		}
		
		public void status() {
			logger.info("trendingDyscoQueue:" + dyscosQueue.size());
			logger.info("inputFeedsPerDysco:" + inputFeedsPerDysco.size());
			logger.info("totalRetrievedItems:" + totalRetrievedItems);
		}
		
		/**
		 * Transforms Query instances to KeywordsFeed instances that will be used 
		 * for searching social media
		 * @param queries
		 * @param dateToRetrieve
		 * @return the list of feeds
		 */
		private List<Feed> transformQueriesToKeywordsFeeds(List<Query> queries, Date dateToRetrieve) {	
			List<Feed> feeds = new ArrayList<Feed>();
			
			for(Query query : queries) {
				UUID UUid = UUID.randomUUID(); 
				Keyword keyword = new Keyword(query.getName(), query.getScore());
				KeywordsFeed feed = new KeywordsFeed(keyword, dateToRetrieve, UUid.toString());
				feeds.add(feed);
			}
			return feeds;
		}

		@Override
		protected void update() {
			// Nothing to update in Trending Dyscos
		}
		
	}