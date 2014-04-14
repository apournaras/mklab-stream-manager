package eu.socialsensor.sfc.streams.store;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.mongodb.MongoException;

import eu.socialsensor.framework.client.dao.ItemDAO;
import eu.socialsensor.framework.client.dao.MediaItemDAO;
import eu.socialsensor.framework.client.dao.StreamUserDAO;
import eu.socialsensor.framework.client.dao.WebPageDAO;
import eu.socialsensor.framework.client.dao.impl.ItemDAOImpl;
import eu.socialsensor.framework.client.dao.impl.MediaItemDAOImpl;
import eu.socialsensor.framework.client.dao.impl.StreamUserDAOImpl;
import eu.socialsensor.framework.client.dao.impl.WebPageDAOImpl;
import eu.socialsensor.framework.client.mongo.MongoHandler;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.MediaItem;
import eu.socialsensor.framework.common.domain.StreamUser;
import eu.socialsensor.framework.common.domain.WebPage;
import eu.socialsensor.sfc.streams.StorageConfiguration;
/**
 * Class for storing items in mongo db
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 * 
 * @author ailiakop
 * @email  ailiakop@iti.gr
 *
 */
public class MongoDbStorage implements StreamUpdateStorage {

	private static String HOST = "mongodb.host";
	private static String DB = "mongodb.database";
	
	private static String ITEMS_DATABASE = "mongodb.items.dbname";
	private static String ITEMS_COLLECTION = "mongodb.items.collection";
	
	private static String MEDIA_ITEMS_DATABASE = "mongodb.mediaitems.dbname";
	private static String MEDIA_ITEMS_COLLECTION = "mongodb.mediaitems.collection";
	
	private static String USERS_DATABASE = "mongodb.streamusers.dbname";
	private static String USERS_COLLECTION = "mongodb.streamusers.collection";
	
	private static String WEBPAGES_DATABASE = "mongodb.webpages.dbname";
	private static String WEBPAGES_COLLECTION = "mongodb.webpages.collection";
	
	private Logger  logger = Logger.getLogger(MongoDbStorage.class);
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
	 
	}
	private String storageName = "Mongodb";
	
	private String host;
	private String dbName;
	
	private String itemsDbName;
	private String itemsCollectionName;
	
	private String mediaItemsDbName;
	private String mediaItemsCollectionName;
	
	private String streamUsersDbName;
	private String streamUsersCollectionName;
	
	private String webPageDbName;
	private String webPageCollectionName;
	
	private ItemDAO itemDAO;
	private MediaItemDAO mediaItemDAO;
	private StreamUserDAO streamUserDAO;
	private WebPageDAO webPageDAO;
	
	private Integer items = 0;
	private long t;
	
	private HashMap<String, Integer> usersMentionsMap, usersItemsMap, usersSharesMap;
	private UpdaterThread updaterThread;

	
	
	public MongoDbStorage(StorageConfiguration config) {	
		this.host = config.getParameter(MongoDbStorage.HOST);
		this.dbName = config.getParameter(MongoDbStorage.DB);
		
		this.itemsDbName = config.getParameter(MongoDbStorage.ITEMS_DATABASE);
		this.itemsCollectionName = config.getParameter(MongoDbStorage.ITEMS_COLLECTION);
		
		this.mediaItemsDbName = config.getParameter(MongoDbStorage.MEDIA_ITEMS_DATABASE);
		this.mediaItemsCollectionName = config.getParameter(MongoDbStorage.MEDIA_ITEMS_COLLECTION);
		
		this.streamUsersDbName = config.getParameter(MongoDbStorage.USERS_DATABASE);
		this.streamUsersCollectionName = config.getParameter(MongoDbStorage.USERS_COLLECTION);
		
		this.webPageDbName = config.getParameter(MongoDbStorage.WEBPAGES_DATABASE);
		this.webPageCollectionName = config.getParameter(MongoDbStorage.WEBPAGES_COLLECTION);
	
		this.usersMentionsMap = new HashMap<String, Integer>();
		this.usersItemsMap = new HashMap<String, Integer>();
		this.usersSharesMap = new HashMap<String, Integer>();
		
		this.items = 0;
	}
	
	public MongoDbStorage(String hostname, String itemsDbName, String itemsCollectionName, String mediaItemsDbName, 
			String mediaItemsCollectionName, String streamUsersDbName, String streamUsersCollectionName, 
			String webPageDbName, String webPageCollectionName) {	
		
		this.host = hostname;
		
		this.itemsDbName = itemsDbName;
		this.itemsCollectionName = itemsCollectionName;
		
		this.mediaItemsDbName = mediaItemsDbName;
		this.mediaItemsCollectionName = mediaItemsCollectionName;
		
		this.streamUsersDbName = streamUsersDbName;
		this.streamUsersCollectionName = streamUsersCollectionName;
		
		this.webPageDbName = webPageDbName; 
		this.webPageCollectionName = webPageCollectionName; 
		
		this.usersMentionsMap = new HashMap<String, Integer>();
		this.usersItemsMap = new HashMap<String, Integer>();
		this.usersSharesMap = new HashMap<String, Integer>();
		
		this.items = 0;
	}
	
	public MongoDbStorage(String hostname, String dbName, String itemsCollectionName, 
			String mediaItemsCollectionName, String streamUsersCollectionName, String webPageCollectionName) {	
		
		this.host = hostname;
		this.dbName = dbName;
		
		this.itemsCollectionName = itemsCollectionName;
		this.mediaItemsCollectionName = mediaItemsCollectionName;
		this.streamUsersCollectionName = streamUsersCollectionName;
		this.webPageCollectionName = webPageCollectionName; 
		
		this.usersMentionsMap = new HashMap<String, Integer>();
		this.usersItemsMap = new HashMap<String, Integer>();
		this.usersSharesMap = new HashMap<String, Integer>();
		
		this.items = 0;
	}
	
	@Override
	public void close() {
		updaterThread.stopThread();
	}

	@Override
	public boolean delete(String id) throws IOException {
		return itemDAO.deleteItem(id);
	}
	
	@Override
	public boolean open() {
		
		logger.info("Open MongoDB storage <host: " + host + ">");

		this.t = System.currentTimeMillis();
		
		if(this.dbName != null){
			try {
				if(itemsCollectionName != null)
					this.itemDAO = new ItemDAOImpl(host, dbName, itemsCollectionName);
				
				if(mediaItemsCollectionName != null)
					this.mediaItemDAO = new MediaItemDAOImpl(host, dbName, mediaItemsCollectionName);
				
				if(streamUsersCollectionName != null)
					this.streamUserDAO = new StreamUserDAOImpl(host, dbName, streamUsersCollectionName);
				
				if(webPageCollectionName != null)
					this.webPageDAO = new WebPageDAOImpl(host, dbName, webPageCollectionName);
			} catch (Exception e) {
				System.err.println("Open MongoDB failed");
				return false;
			}
		}
		else{
		
			try {
				if(itemsCollectionName != null)
					this.itemDAO = new ItemDAOImpl(host, itemsDbName, itemsCollectionName);
				
				if(mediaItemsCollectionName != null)
					this.mediaItemDAO = new MediaItemDAOImpl(host, mediaItemsDbName, mediaItemsCollectionName);
				
				if(streamUsersCollectionName != null)
					this.streamUserDAO = new StreamUserDAOImpl(host, streamUsersDbName, streamUsersCollectionName);
				
				if(webPageCollectionName != null)
					this.webPageDAO = new WebPageDAOImpl(host, webPageDbName, webPageCollectionName);
			} catch (Exception e) {
				System.err.println("Open MongoDB failed");
				return false;
			}
		}
		
		this.updaterThread = new UpdaterThread();
		updaterThread.start();
		
		return true;
		
	}

	
	@Override
	public void store(Item item) {
		
		try {
			
			if((++items%500) == 0) {
				logger.info("Mongo I/O rate: " + 500000/(System.currentTimeMillis()-t) + " items/sec");
				t = System.currentTimeMillis();
			}
			
			// Handle Items
			if(!itemDAO.exists(item.getId())) {
				// save item
				item.setLastUpdated(new Date());
				item.setInsertionTime(System.currentTimeMillis());
				itemDAO.insertItem(item);
				
				// Handle Stream Users
				StreamUser user = item.getStreamUser();
				if(user != null) {
					if(!streamUserDAO.exists(user.getId())) {
						// save stream user
						streamUserDAO.insertStreamUser(user);
					}
					else {
						//streamUserDAO.incStreamUserValue(user.getId(), "items");
						//streamUserDAO.incStreamUserValue(user.getId(), "mentions");
						
						synchronized(usersItemsMap) {
							Integer items = usersItemsMap.get(user.getId());
							if(items == null)
								items = 0;
							usersItemsMap.put(user.getId(), ++items);
						}
						
						synchronized(usersMentionsMap) {
							Integer mentions = usersMentionsMap.get(user.getId());
							if(mentions == null)
								mentions = 0;
							usersMentionsMap.put(user.getId(), ++mentions);
						}
					}
				}
				
				if(item.getMentions() != null) {
					String[] mentionedUsers = item.getMentions();
					for(String mentionedUser : mentionedUsers) {
						//streamUserDAO.incStreamUserValue(mention, "mentions");
						synchronized(usersMentionsMap) {
							Integer mentions = usersMentionsMap.get(mentionedUser);
							if(mentions == null)
								mentions = 0;
							usersMentionsMap.put(mentionedUser, ++mentions);
						}
					}
				}

				if(item.getReferencedUserId() != null) {
					String userid = item.getReferencedUserId();
					//streamUserDAO.incStreamUserValue(userid, "shares");
					synchronized(usersSharesMap) {
						Integer shares = usersSharesMap.get(userid);
						if(shares == null)
							shares = 0;
						usersSharesMap.put(userid, ++shares);
					}
				}
				
				// Handle Media Items
				for(MediaItem mediaItem : item.getMediaItems()) {
					if(!mediaItemDAO.exists(mediaItem.getId())) {
						// save media item
						mediaItemDAO.addMediaItem(mediaItem);
					}
					else {
						//mediaItemDAO.updateMediaItemPopularity(mediaItem);
					}
				}
				
				// Handle Web Pages
				List<WebPage> webPages = item.getWebPages();
				if(webPages != null) {
					for(WebPage webPage : webPages) {
						String webPageURL = webPage.getUrl();
						if(!webPageDAO.exists(webPageURL)) {
							webPageDAO.addWebPage(webPage);
						}
						else {
							webPageDAO.updateWebPageShares(webPageURL);
						}
					}
				}
			}
			else {
				itemDAO.updateItem(item);
				
			}
		}
		catch(MongoException e){
			System.out.println("Storing item "+item.getId()+" failed - Mongo is not responding");
		}
	
	}

	@Override
	public void update(Item update) throws IOException {
		// update item
		store(update);
	}
	
	@Override
	public void updateTimeslot() {
	}
	
	@Override
	public boolean checkStatus(StreamUpdateStorage storage) 
	{
		try {
			MongoHandler handler = new MongoHandler(host, itemsDbName);
			return handler.checkConnection(host);
		} catch (Exception e) {
			
			return false;
		}
		
	}
	
	@Override
	public String getStorageName(){
		return this.storageName;
	}
	
	private class UpdaterThread extends Thread {

		private boolean stop = false;

		@Override
		public void run() {
			while(!stop) {
				try {
					synchronized(usersMentionsMap) {
						for(Entry<String, Integer> e : usersMentionsMap.entrySet()) {
							streamUserDAO.incStreamUserValue(e.getKey(), "mentions", e.getValue());
						}
						usersMentionsMap.clear();
					}
					
					synchronized(usersSharesMap) {
						for(Entry<String, Integer> e : usersSharesMap.entrySet()) {
							streamUserDAO.incStreamUserValue(e.getKey(), "shares", e.getValue());
						}
						usersSharesMap.clear();
					}

					synchronized(usersItemsMap) {
						for(Entry<String, Integer> e : usersItemsMap.entrySet()) {
							streamUserDAO.incStreamUserValue(e.getKey(), "items", e.getValue());
						}
						usersItemsMap.clear();
					}

					Thread.sleep(10*60*1000);
				} catch (InterruptedException e) {
					continue;
				}
			}
		}
		
		public void stopThread() {
			this.stop = true;
			this.interrupt();
		}
	}
}
