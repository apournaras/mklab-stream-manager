package eu.socialsensor.sfc.streams.store;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import eu.socialsensor.framework.client.dao.ItemDAO;
import eu.socialsensor.framework.client.dao.MediaItemDAO;
import eu.socialsensor.framework.client.dao.impl.ItemDAOImpl;
import eu.socialsensor.framework.client.dao.impl.MediaItemDAOImpl;
import eu.socialsensor.framework.client.search.solr.SolrMediaItemHandler;
import eu.socialsensor.framework.client.search.solr.SolrTopicDetectionItem;
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
	private static final String TOPICDETECTIONITEMS_COLLECTION = "solr.topicDetectionItems.collection";
	private static final String MEDIAITEMS_COLLECTION = "solr.mediaitems.collection";
	
	private String hostname, service;
	private String topicDetectionItemsCollection = null;
	private String mediaItemsCollection = null;
	
	private HttpSolrServer topicDetectionItemsServer = null;
	private SolrMediaItemHandler solrMediaHandler = null; 
	
	public SolrStorage(StorageConfiguration config) throws IOException {
		this.hostname = config.getParameter(SolrStorage.HOSTNAME);
		this.service = config.getParameter(SolrStorage.SERVICE);
		this.mediaItemsCollection = config.getParameter(SolrStorage.MEDIAITEMS_COLLECTION);
		this.topicDetectionItemsCollection = config.getParameter(SolrStorage.TOPICDETECTIONITEMS_COLLECTION);
	}
	
	public SolrMediaItemHandler getHandler(){
		return solrMediaHandler;
	}
	
	@Override
	public void open() throws IOException {
		if(topicDetectionItemsCollection != null) {
			topicDetectionItemsServer = new HttpSolrServer(hostname+"/"+service+"/"+topicDetectionItemsCollection);
		}
		if(mediaItemsCollection != null) {	
			solrMediaHandler = SolrMediaItemHandler.getInstance(hostname+"/"+service+"/"+mediaItemsCollection);
		}
	}

	@Override
	public void store(Item item) throws IOException {
		SolrTopicDetectionItem solrTopicDetectionItem = new SolrTopicDetectionItem(item);
		try {
			if(topicDetectionItemsServer != null) {
				topicDetectionItemsServer.addBean(solrTopicDetectionItem);	
			}
			
			if(solrMediaHandler != null) {
				
				for(Entry<URL, MediaItem> entry : item.getMediaItems().entrySet()) {
					
					MediaItem mediaItem = new MediaItem(entry.getKey(), item);
					//System.out.println(mediaItem.toJSONString());
					
					MediaItem mi = solrMediaHandler.getSolrMediaItem(mediaItem.getId());
					if(mi==null) {
						//System.out.println("New media item");
						solrMediaHandler.insertMediaItem(mediaItem, null);
					}
					else {
						//System.out.println("Media item "+mi.getId()+" already exists - update");
				
						
						for(String key : mediaItem.getFeedKeywords()){
							if(!mi.getFeedKeywords().contains(key)){
								//System.out.println("Add keyword : "+key);
								mi.getFeedKeywords().add(key);
							}
								
						}
						for(String key : mediaItem.getFeedKeywordsString()) {
							if(!mi.getFeedKeywordsString().contains(key))
								mi.getFeedKeywordsString().add(key);
						}
						solrMediaHandler.insertMediaItem(mi, null);
					}
				}
			}
		} catch (SolrServerException e) {
			throw new IOException(e);
		}
		
	}

	@Override
	public boolean delete(String itemId) throws IOException {
		logger.info("Delete item with id " + itemId + " from Solr.");
		try {
			if(topicDetectionItemsServer != null) {
				topicDetectionItemsServer.deleteById(itemId);
			}
		} catch (SolrServerException e) {
			return false;
		}
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
			topicDetectionItemsServer.optimize();
			topicDetectionItemsServer.commit();
		} catch (SolrServerException e) {
			
		}
	}
	
	public static void main(String[] args) throws IOException {
		
		ItemDAO itemDAO = new ItemDAOImpl();
		MediaItemDAO mediaItemDAO = new MediaItemDAOImpl();
		
		System.out.println("DEBUG SOLR STORAGE");
		
		List<Item> items = itemDAO.getLatestItems(1);
		List<MediaItem> mitems = mediaItemDAO.getLastMediaItems(1);
		
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
