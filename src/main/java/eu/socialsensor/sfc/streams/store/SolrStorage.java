package eu.socialsensor.sfc.streams.store;

import java.io.IOException;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.client.search.solr.SolrItemHandler;
import eu.socialsensor.framework.client.search.solr.SolrMediaItemHandler;
import eu.socialsensor.framework.client.search.solr.SolrNewsFeedHandler;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.MediaItem;
import eu.socialsensor.sfc.streams.StorageConfiguration;

/**
 * Class for storing items to solr
 * @author manosetro
 * @email  manosetro@iti.gr
 *
 */
public class SolrStorage implements StreamUpdateStorage {

	private Logger  logger = Logger.getLogger(SolrStorage.class);
	
	private static final String HOSTNAME = "solr.hostname";
	private static final String SERVICE = "solr.service";
	
	private static final String ITEMS_COLLECTION = "solr.items.collection";
	private static final String MEDIAITEMS_COLLECTION = "solr.mediaitems.collection";
	private static final String NEWSFEED_COLLECTION = "solr.newsfeed.collection";
	
	private String hostname, service;
	
	private String itemsCollection = null;
	private String mediaItemsCollection = null;
	private String newsFeedCollection = null;
	
	private SolrItemHandler solrItemHandler = null; 
	private SolrMediaItemHandler solrMediaHandler = null;
	private SolrNewsFeedHandler solrNewsFeedHandler = null;
	
	public SolrStorage(StorageConfiguration config) throws IOException {
		this.hostname = config.getParameter(SolrStorage.HOSTNAME);
		this.service = config.getParameter(SolrStorage.SERVICE);
		this.itemsCollection = config.getParameter(SolrStorage.ITEMS_COLLECTION);
		this.mediaItemsCollection = config.getParameter(SolrStorage.MEDIAITEMS_COLLECTION);
		this.newsFeedCollection = config.getParameter(SolrStorage.NEWSFEED_COLLECTION);
	}
	
	public SolrItemHandler getItemHandler() {
		return solrItemHandler;
	}
	
	public SolrMediaItemHandler getMediaItemHandler() {
		return solrMediaHandler;
	}
	
	public SolrNewsFeedHandler getNewsFeedHandler() {
		return solrNewsFeedHandler;
	}
	
	@Override
	public void open() throws IOException {
		
		if(itemsCollection != null) {	
			solrItemHandler = SolrItemHandler.getInstance(hostname+"/"+service+"/"+itemsCollection);
		}
		if(mediaItemsCollection != null) {	
			solrMediaHandler = SolrMediaItemHandler.getInstance(hostname+"/"+service+"/"+mediaItemsCollection);
		}
		if(newsFeedCollection != null) {	
			solrNewsFeedHandler = SolrNewsFeedHandler.getInstance(hostname+"/"+service+"/"+newsFeedCollection);
		}
	}

	@Override
	public void store(Item item) throws IOException {
	
		// Index only original Items and MediaItems come from original Items
		if(!item.isOriginal())
			return;
		
		if(solrItemHandler != null) {
			solrItemHandler.insertItem(item);
		}
		
		if(solrNewsFeedHandler != null){
			solrNewsFeedHandler.insertItem(item);
		}
		
		if(solrMediaHandler != null) {
			
			for(MediaItem mediaItem : item.getMediaItems()) {
				MediaItem mi = solrMediaHandler.getSolrMediaItem(mediaItem.getId());
				
				if(mi==null) {
					solrMediaHandler.insertMediaItem(mediaItem);
				}
				else {
					
					for(String key : mediaItem.getFeedKeywords()){
						if(!mi.getFeedKeywords().contains(key)){
							mi.getFeedKeywords().add(key);
						}
							
					}
					for(String key : mediaItem.getFeedKeywordsString()) {
						if(!mi.getFeedKeywordsString().contains(key))
							mi.getFeedKeywordsString().add(key);
					}
					solrMediaHandler.insertMediaItem(mi);
				}
			}
		}
		
	}
	

	@Override
	public void update(Item update) throws IOException {
		store(update);
	}
	
	@Override
	public boolean delete(String itemId) throws IOException {
		//logger.info("Delete item with id " + itemId + " from Solr.");
		solrItemHandler.deleteItem(itemId);
		return true;
	}
	

	@Override
	public void close() {
		try {
			commit();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void updateTimeslot() {
		try {
			commit();
		} catch (Exception e) {
			logger.error(e);
		}
	}
	
	private void commit() throws IOException {
		try {
			commit();
		} catch (Exception e) {
			logger.error(e);
		}
	}
	
	public static void main(String[] args) throws IOException {
		
	}
	
}
