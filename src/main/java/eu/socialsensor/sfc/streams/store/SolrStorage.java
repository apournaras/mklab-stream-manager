package eu.socialsensor.sfc.streams.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.client.search.solr.SolrItemHandler;
import eu.socialsensor.framework.client.search.solr.SolrMediaItemHandler;
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
	
	private String hostname, service;
	private String itemsCollection = null;
	private String mediaItemsCollection = null;
	
	private SolrItemHandler solrItemHandler = null; 
	private SolrMediaItemHandler solrMediaHandler = null; 
	
	public SolrStorage(StorageConfiguration config) throws IOException {
		this.hostname = config.getParameter(SolrStorage.HOSTNAME);
		this.service = config.getParameter(SolrStorage.SERVICE);
		this.itemsCollection = config.getParameter(SolrStorage.ITEMS_COLLECTION);
		this.mediaItemsCollection = config.getParameter(SolrStorage.MEDIAITEMS_COLLECTION);
	}
	
	public SolrMediaItemHandler getHandler(){
		return solrMediaHandler;
	}
	
	@Override
	public void open() throws IOException {
		if(itemsCollection != null) {	
			solrItemHandler = SolrItemHandler.getInstance(hostname+"/"+service+"/"+itemsCollection);
		}
		if(mediaItemsCollection != null) {	
			solrMediaHandler = SolrMediaItemHandler.getInstance(hostname+"/"+service+"/"+mediaItemsCollection);
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
	
	}
	
	public static void main(String[] args) throws IOException {
		System.out.println("DEBUG SOLR STORAGE");
		
		StorageConfiguration conf = new StorageConfiguration();
		conf.setParameter(HOSTNAME, "http://160.40.50.230:8080");
		conf.setParameter(SERVICE, "solr");
		conf.setParameter(MEDIAITEMS_COLLECTION, "DyscoMediaItems");
		
		SolrStorage solrStorage = new SolrStorage(conf);
		solrStorage.open();
		List<String> keywords = new ArrayList<String>();
		keywords.add("kidnapper");
		keywords.add("ohio");
		keywords.add("ariel castro");
		List<MediaItem> mediaItems = solrStorage.getHandler().findAllMediaItemsByKeywords(keywords,"image",100);
		System.out.println("I have retrieved "+mediaItems.size()+" media items");
//		for(MediaItem mItem : mediaItems)
//			System.out.println(mItem.toJSONString());
		/*Item item = items.get(0);
		MediaItem mi = mitems.get(0);
		
		Map<URL, MediaItem> mediaItems = new HashMap<URL, MediaItem>();
		mi.setId("Twitter::12345678");
		
		mediaItems.put(new URL(mi.getUrl()), mi);
		
		item.setMediaItems(mediaItems );
		
		System.out.println(items.get(0).toJSONString());
		
		solrStorage.store(items.get(0));*/
	}
	
}
