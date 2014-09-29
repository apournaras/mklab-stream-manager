package eu.socialsensor.sfc.streams.search;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.Keyword;
import eu.socialsensor.framework.common.domain.dysco.Dysco;
import eu.socialsensor.framework.common.domain.dysco.Entity;
import eu.socialsensor.framework.common.domain.dysco.Entity.Type;
import eu.socialsensor.framework.common.domain.feeds.KeywordsFeed;
import eu.socialsensor.framework.common.util.DateUtil;
import eu.socialsensor.sfc.streams.monitors.StreamsMonitor;

public abstract class SearchHandler extends Thread {

	protected Logger logger = Logger.getLogger(SearchHandler.class);
	protected StreamsMonitor monitor = null;
	
	private boolean isAlive = true;
	private long totalRetrievedItems = 0;
	
	protected BlockingQueue<Dysco> dyscosQueue = new LinkedBlockingDeque<Dysco>();
	
	public SearchHandler(StreamsMonitor monitor) {
		this.monitor = monitor;
	}
	
	protected List<Item> search(List<Feed> feeds) {
		return search(feeds, null);
	}
	
	/**
	 * Searches in all social media defined in the configuration file
	 * for the list of feeds that is given as input and returns the retrieved items
	 * @param feeds
	 * @param streamsToSearch
	 * @return the list of the items retrieved
	 */
	protected synchronized List<Item> search(List<Feed> feeds, Set<String> streams) {
		List<Item> items = new ArrayList<Item>();
		if(feeds != null && !feeds.isEmpty()) {
			try {
				if(streams == null)
					monitor.retrieve(feeds);	
				else
					monitor.retrieve(streams, feeds);
				
				while(!monitor.areStreamsFinished()) {
					try {
						Thread.sleep(50);
					} catch (InterruptedException e) {
						logger.error(e.getMessage());
					}
				}		
					
				items.addAll(monitor.getTotalRetrievedItems());
				totalRetrievedItems += items.size();
					
				monitor.reset();
			} catch (Exception e) {
				logger.error(e.getMessage());
				monitor.reset();
			}
		}
		return items;
	}
	
	/**
	 * Transforms Query instances to KeywordsFeed instances that will be used 
	 * for searching social media
	 * @param queries
	 * @param dateToRetrieve
	 * @return the list of feeds
	 */
	public List<Feed> getSimpleFeeds(Dysco dysco) {
		
		List<Feed> feeds = new ArrayList<Feed>();
		
		DateUtil dateUtil = new DateUtil();
		Date dateToRetrieve = dateUtil.addDays(dysco.getCreationDate(), -1);
		
		Map<String, Double> keywords = dysco.getKeywords();
		for(Entry<String, Double> entry : keywords.entrySet()) {
			
			String text = entry.getKey();
			String[] parts = text.split("\\s+");
			
			if(parts.length < 2)
				continue;
			
			UUID UUid = UUID.randomUUID(); 
			Keyword keyword = new Keyword(text, entry.getValue());
			KeywordsFeed feed = new KeywordsFeed(keyword, dateToRetrieve, UUid.toString());
			feeds.add(feed);
		}
		
		List<Entity> entities = dysco.getEntities();
		for(Entity entity : entities) {
			if(entity.getType().equals(Type.LOCATION))
				continue;
			
			UUID UUid = UUID.randomUUID(); 
			Keyword keyword = new Keyword(entity.getName(), entity.getCont());
			KeywordsFeed feed = new KeywordsFeed(keyword, dateToRetrieve, UUid.toString());
			feeds.add(feed);
		}
		
		Map<String, Double> hashtags = dysco.getHashtags();
		for(Entry<String, Double> entry : hashtags.entrySet()) {
			
			String hashtag = entry.getKey();
			hashtag = hashtag.replaceAll("#", "");
			
			if(hashtag.length() < 4)
				continue;
			
			UUID UUid = UUID.randomUUID(); 
			Keyword keyword = new Keyword(hashtag, entry.getValue());
			KeywordsFeed feed = new KeywordsFeed(keyword, dateToRetrieve, UUid.toString());
			feeds.add(feed);
		}

		return feeds;
	}
	
	public void addDysco(Dysco dysco) {
		try {
			dyscosQueue.put(dysco);
			logger.info(dysco.getId() + " putted in dyscos queue (" + dyscosQueue.size() + ")");
		}
		catch(Exception e) {
			logger.error(e);
		}
	}
	
	public abstract void deleteDysco(String dyscoId);
	
	public void run() {
		while(isAlive) {
			update(); 
			Dysco dysco = dyscosQueue.poll();
			if(dysco == null) {
				try {
					synchronized(this) {
						this.wait(100);
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
					logger.error("Exception: " + e.getMessage());
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
	
	public void status() {
		logger.info("DyscoQueue:" + dyscosQueue.size());
		logger.info("totalRetrievedItems:" + totalRetrievedItems);
		
		if(dyscosQueue.size() > 500) {
			dyscosQueue.clear();
		}
	}
	
	protected abstract void searchForDysco(Dysco dysco);
	
	protected abstract void update();
		
}
