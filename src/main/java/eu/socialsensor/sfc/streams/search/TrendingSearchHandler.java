package eu.socialsensor.sfc.streams.search;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.Keyword;
import eu.socialsensor.framework.common.domain.Query;
import eu.socialsensor.framework.common.domain.dysco.Dysco;
import eu.socialsensor.framework.common.domain.feeds.KeywordsFeed;
import eu.socialsensor.sfc.builder.SolrQueryBuilder;

/**
	 * Class responsible for Trending DySco requests 
	 * It performs custom search to retrieve an item collection
	 * based on Dysco's primary queries and afterwards applies query
	 * expansion via the Query Builder module to extend the search
	 * and detect more relative content to the DySco. 
	 * 
	 * @author ailiakop
	 */
	public class TrendingSearchHandler extends Thread {
		
		public final Logger logger = Logger.getLogger(TrendingSearchHandler.class);
		
		private SolrQueryBuilder queryBuilder;
		
		private BlockingQueue<Dysco> trendingDyscosQueue = new LinkedBlockingDeque<Dysco>();
		private Map<String, List<Feed>> inputFeedsPerDysco = new ConcurrentHashMap<String, List<Feed>>();
		
		private List<Item> retrievedItems = new ArrayList<Item>();
		
		private MediaSearcher searcher;

		private boolean isAlive = true;
		
		private Date retrievalDate;

		private Queue<Dysco> dyscosToUpdate; 

		public TrendingSearchHandler(MediaSearcher mediaSearcher, Queue<Dysco> dyscosToUpdate) {
			this.searcher = mediaSearcher;
			this.dyscosToUpdate = dyscosToUpdate;
			
			try {
				this.queryBuilder = new SolrQueryBuilder();
			} catch (Exception e) {
				logger.error(e);
			}
			
		}
		
		public void addTrendingDysco(Dysco dysco, List<Feed> inputFeeds) {
			try {
				logger.info("New incoming Trending DySco: " + dysco.getId() + " with " + inputFeeds.size() + " searchable feeds");
				trendingDyscosQueue.put(dysco);
				inputFeedsPerDysco.put(dysco.getId(), inputFeeds);
				logger.info("Putted in dyscos queue (" + trendingDyscosQueue.size() + ")");
			}
			catch(Exception e) {
				logger.error(e);
			}
		}
		
		public void run() {
			Dysco dysco = null;
			while(isAlive) {
				dysco = poll();
				if(dysco == null) {
					try {
						synchronized(this) {
							this.wait(5000);
						}
					} catch (InterruptedException e) {
						logger.error(e);
					}
					continue;
				}
				else {
					try {
						searchForTrendingDysco(dysco);
					}
					catch(Exception e) {
						logger.error("Error during searching for trending dysco " + dysco.getId(), e);
					}
				}
			}
		}
		
		/**
		 * Polls a trending DySco request from the queue to search
		 * @return
		 */
		private Dysco poll() {
			Dysco request = trendingDyscosQueue.poll();
			return request;
		}
		
		/**
		 * Searches for a Trending DySco at two stages. At the first stage the algorithm 
		 * retrieves content from social media using DySco's primal queries, whereas 
		 * at the second stage it uses the retrieved content to expand the queries.
		 * The final queries, which are the result of merging the primal and the expanded
		 * queries are used to search further in social media for additional content. 
		 * @param dysco
		 */
		private synchronized void searchForTrendingDysco(Dysco dysco) {
			
			String dyscoId = dysco.getId();
			
			long start = System.currentTimeMillis();
			logger.info("Media Searcher handling #" + dyscoId);
			
			//first search
			List<Feed> feeds = inputFeedsPerDysco.remove(dyscoId);
			
			retrievalDate = new Date(System.currentTimeMillis());
			retrievedItems = searcher.search(feeds);
			
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
				List<Feed> newFeeds = transformQueriesToKeywordsFeeds(expandedQueries, retrievalDate);
				List<Item> secondSearchItems = searcher.search(newFeeds);
				
				long t4 = System.currentTimeMillis();
				logger.info("Total Items retrieved for Trending DySco : " + dyscoId + " : " + secondSearchItems.size());
				logger.info("Time for Second Search for Trending DySco: " + dyscoId + " is " + (t4-t3)/1000 + " sec.");
			
				dysco.setSolrQueries(queries);
				dyscosToUpdate.add(dysco);
			}
			else {
				System.out.println("No queries to update");
			}
			
			long end = System.currentTimeMillis();
			logger.info("Total Time searching for Trending DySco: " + dyscoId + " is " + (end-start)/1000 + " sec.");
		}
		
		
		/**
		 * Stops TrendingSearchHandler
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
		
		public void status() {
			logger.info("trendingDyscoQueue:" + trendingDyscosQueue.size());
			logger.info("inputFeedsPerDysco:" + inputFeedsPerDysco.size());
			logger.info("retrievedItems:" + retrievedItems.size());
		}
		
		/**
		 * Transforms Query instances to KeywordsFeed instances that will be used 
		 * for searching social media
		 * @param queries
		 * @param dateToRetrieve
		 * @return the list of feeds
		 */
		private List<Feed> transformQueriesToKeywordsFeeds(List<Query> queries,Date dateToRetrieve) {	
			List<Feed> feeds = new ArrayList<Feed>();
			
			for(Query query : queries) {
				UUID UUid = UUID.randomUUID(); 
				feeds.add(new KeywordsFeed(new Keyword(query.getName(),query.getScore()),dateToRetrieve,UUid.toString()));
			}
			
			return feeds;
		}
	}