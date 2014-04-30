package eu.socialsensor.sfc.streams.store;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.mongodb.MongoException;

import eu.socialsensor.framework.client.dao.ItemDAO;
import eu.socialsensor.framework.client.dao.MediaItemDAO;
import eu.socialsensor.framework.client.dao.MediaSharesDAO;
import eu.socialsensor.framework.client.dao.StreamUserDAO;
import eu.socialsensor.framework.client.dao.WebPageDAO;
import eu.socialsensor.framework.client.dao.impl.ItemDAOImpl;
import eu.socialsensor.framework.client.dao.impl.MediaItemDAOImpl;
import eu.socialsensor.framework.client.dao.impl.MediaSharesDAOImpl;
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

	private static String ITEMS_DATABASE = "mongodb.items.database";
	private static String ITEMS_COLLECTION = "mongodb.items.collection";
	
	private static String MEDIA_ITEMS_DATABASE = "mongodb.mediaitems.database";
	private static String MEDIA_ITEMS_COLLECTION = "mongodb.mediaitems.collection";
	
	private static String MEDIA_SHARES_DATABASE = "mongodb.mediashares.database";
	private static String MEDIA_SHARES_COLLECTION = "mongodb.mediashares.collection";
	
	private static String USERS_DATABASE = "mongodb.streamusers.database";
	private static String USERS_COLLECTION = "mongodb.streamusers.collection";
	
	private static String WEBPAGES_DATABASE = "mongodb.webpages.database";
	private static String WEBPAGES_COLLECTION = "mongodb.webpages.collection";
	
	private Logger logger = Logger.getLogger(MongoDbStorage.class);
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
	 
	}
	
	private String storageName = "Mongodb";
	
	private String host;
	private String database;
	
	private String itemsDbName;
	private String itemsCollectionName;
	
	private String mediaItemsDbName;
	private String mediaItemsCollectionName;
	
	private String mediaSharesDbName;
	private String mediaSharesCollectionName;
	
	private String streamUsersDbName;
	private String streamUsersCollectionName;
	
	private String webPageDbName;
	private String webPageCollectionName;
	
	private ItemDAO itemDAO = null;
	private MediaItemDAO mediaItemDAO = null;
	private MediaSharesDAO mediaSharesDAO = null;
	private StreamUserDAO streamUserDAO = null;
	private WebPageDAO webPageDAO = null;
	
	private Integer items = 0, pItems = 0, mediaItems = 0, wPages = 0, users = 0;
	private long t;
	
	private HashMap<String, Integer> usersMentionsMap, usersItemsMap, usersSharesMap, webpagesSharesMap;
	private HashMap<String, Item> itemsMap;
	
	private UpdaterThread updaterThread;
	
	public MongoDbStorage(StorageConfiguration config) {	
		this.host = config.getParameter(MongoDbStorage.HOST);
		this.database = config.getParameter(MongoDbStorage.DB);
		
		this.itemsDbName = config.getParameter(MongoDbStorage.ITEMS_DATABASE);
		this.itemsCollectionName = config.getParameter(MongoDbStorage.ITEMS_COLLECTION);
		
		this.mediaItemsDbName = config.getParameter(MongoDbStorage.MEDIA_ITEMS_DATABASE);
		this.mediaItemsCollectionName = config.getParameter(MongoDbStorage.MEDIA_ITEMS_COLLECTION);
		
		this.mediaSharesDbName = config.getParameter(MongoDbStorage.MEDIA_SHARES_DATABASE);
		this.mediaSharesCollectionName = config.getParameter(MongoDbStorage.MEDIA_SHARES_COLLECTION);
		
		this.streamUsersDbName = config.getParameter(MongoDbStorage.USERS_DATABASE);
		this.streamUsersCollectionName = config.getParameter(MongoDbStorage.USERS_COLLECTION);
		
		this.webPageDbName = config.getParameter(MongoDbStorage.WEBPAGES_DATABASE);
		this.webPageCollectionName = config.getParameter(MongoDbStorage.WEBPAGES_COLLECTION);
	
		this.itemsMap = new HashMap<String, Item>();
		
		this.usersMentionsMap = new HashMap<String, Integer>();
		this.usersItemsMap = new HashMap<String, Integer>();
		this.usersSharesMap = new HashMap<String, Integer>();
		this.webpagesSharesMap = new HashMap<String, Integer>();
		
		this.pItems = 0;
		this.items = 0;
		this.mediaItems = 0;
		this.wPages = 0;
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
		this.webpagesSharesMap = new HashMap<String, Integer>();
		
		this.pItems = 0;
		this.items = 0;
		this.mediaItems = 0;
		this.wPages = 0;
	}
	
	public MongoDbStorage(String hostname, String database, String itemsCollectionName,
			String mediaItemsCollectionName, String streamUsersCollectionName, String webPageCollectionName) {	
		
		this.host = hostname;
		this.database = database;
		
		this.itemsCollectionName = itemsCollectionName;
		this.mediaItemsCollectionName = mediaItemsCollectionName;
		this.streamUsersCollectionName = streamUsersCollectionName;
		this.webPageCollectionName = webPageCollectionName; 
		
		this.usersMentionsMap = new HashMap<String, Integer>();
		this.usersItemsMap = new HashMap<String, Integer>();
		this.usersSharesMap = new HashMap<String, Integer>();
		this.webpagesSharesMap = new HashMap<String, Integer>();
		
		this.pItems = 0;
		this.items = 0;
		this.mediaItems = 0;
		this.wPages = 0;
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
		
		if(database != null) {
			try {
				if(itemsCollectionName != null)
					this.itemDAO = new ItemDAOImpl(host, database, itemsCollectionName);
				
				if(mediaItemsCollectionName != null)
					this.mediaItemDAO = new MediaItemDAOImpl(host, database, mediaItemsCollectionName);
				
				if(mediaSharesCollectionName != null)
					this.mediaSharesDAO = new MediaSharesDAOImpl(host, database, mediaSharesCollectionName);
				
				if(streamUsersCollectionName != null)
					this.streamUserDAO = new StreamUserDAOImpl(host, database, streamUsersCollectionName);
				
				if(webPageCollectionName != null)
					this.webPageDAO = new WebPageDAOImpl(host, database, webPageCollectionName);
			} catch (Exception e) {
				logger.error("MongoDB Storage failed to open!");
				logger.error(e);
				return false;
			}
		}
		else {
			try {
				if(itemsCollectionName != null && itemsDbName != null)
					this.itemDAO = new ItemDAOImpl(host, itemsDbName, itemsCollectionName);
				
				if(mediaItemsCollectionName != null && mediaItemsDbName != null)
					this.mediaItemDAO = new MediaItemDAOImpl(host, mediaItemsDbName, mediaItemsCollectionName);
				
				if(mediaSharesCollectionName != null && mediaSharesDbName != null)
					this.mediaSharesDAO = new MediaSharesDAOImpl(host, mediaSharesDbName, mediaSharesCollectionName);
				
				if(streamUsersCollectionName != null && streamUsersDbName != null)
					this.streamUserDAO = new StreamUserDAOImpl(host, streamUsersDbName, streamUsersCollectionName);
				
				if(webPageCollectionName != null && webPageDbName != null)
					this.webPageDAO = new WebPageDAOImpl(host, webPageDbName, webPageCollectionName);
			} catch (Exception e) {
				logger.error("MongoDB Storage failed to open!");
				logger.error(e);
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
			// Handle Items
			String itemId = item.getId();
			
			boolean itemExists = false;
			synchronized(itemsMap) {
				itemExists = itemsMap.containsKey(itemId) || itemDAO.exists(itemId);
			}
			
			if(!itemExists) {
				
				// Item does not exist in MongoDB. Save it.
				items++;
				
				item.setInsertionTime(System.currentTimeMillis());
				itemDAO.insertItem(item);
				
				// Handle Stream Users
				StreamUser user = item.getStreamUser();
				if(user != null) {
					String userId = user.getId();
					boolean userExists = false;
					synchronized(usersItemsMap) {
						userExists = usersItemsMap.containsKey(userId) || streamUserDAO.exists(userId);
					}
					if(!userExists) {
						// save stream user
						users++;
						streamUserDAO.insertStreamUser(user);
					}
					else {
						// Update statistics of stream user
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
						// MediaItem does not exist. Save it.
						
						mediaItems++;
						mediaItemDAO.addMediaItem(mediaItem);
					}
					else {
						//Update media item
						//mediaItemDAO.updateMediaItemPopularity(mediaItem);
					}
					
					if(mediaSharesDAO != null) {
						mediaSharesDAO.addMediaShare(mediaItem.getId(), mediaItem.getRef(), 
								mediaItem.getPublicationTime(), mediaItem.getUserId());
					}
				}
				
				// Handle Web Pages
				List<WebPage> webPages = item.getWebPages();
				if(webPages != null) {
					for(WebPage webPage : webPages) {
						String webPageURL = webPage.getUrl();
						if(!webPageDAO.exists(webPageURL)) {
							
							wPages++;
							webPageDAO.addWebPage(webPage);
						}
						else {
							synchronized(webpagesSharesMap) {
								Integer shares = webpagesSharesMap.get(webPageURL);
								if(shares == null)
									shares = 0;
								webpagesSharesMap.put(webPageURL, ++shares);
							}
						}
					}
				}
			}
			else {
				synchronized(itemsMap) {
					itemsMap.put(item.getId(), item);
				}
			}
		}
		catch(MongoException e) {
			e.printStackTrace();
			System.out.println("Storing item "+item.getId()+" failed.");
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
	public boolean checkStatus(StreamUpdateStorage storage) {
		try {
			logger.info(items + " items, " + mediaItems + " media items " + users + " stream users and " 
					+ wPages + " web pages stored in mongodb");
			logger.info("Mongo I/O rate: " + (items-pItems)/((System.currentTimeMillis()-t)/60000) + " items/min");
			pItems = items;
			t = System.currentTimeMillis();
			
			String testDB = (database != null) ? database : itemsDbName;
			MongoHandler handler = new MongoHandler(host, testDB);
			return handler.checkConnection(host);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e);
			return false;
		}
	}
	
	@Override
	public boolean deleteItemsOlderThan(long dateThreshold) throws IOException {
		return true;
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
					Thread.sleep(10 * 60 * 1000);
					
					logger.info("Update: ");
					long t = System.currentTimeMillis();
					
					synchronized(itemsMap) {
						logger.info(itemsMap.size() + " items");
						for(Item item : itemsMap.values()) {
							itemDAO.updateItem(item);
						}
						itemsMap.clear();
					}
					
					synchronized(usersMentionsMap) {
						logger.info(usersMentionsMap.size() + " mentioned user");
						for(Entry<String, Integer> e : usersMentionsMap.entrySet()) {
							streamUserDAO.incStreamUserValue(e.getKey(), "mentions", e.getValue());
						}
						usersMentionsMap.clear();
					}
					
					synchronized(usersSharesMap) {
						logger.info(usersSharesMap.size() + " user shares");
						for(Entry<String, Integer> e : usersSharesMap.entrySet()) {
							streamUserDAO.incStreamUserValue(e.getKey(), "shares", e.getValue());
						}
						usersSharesMap.clear();
					}

					synchronized(usersItemsMap) {
						logger.info(usersItemsMap.size() + " user");
						for(Entry<String, Integer> e : usersItemsMap.entrySet()) {
							streamUserDAO.incStreamUserValue(e.getKey(), "items", e.getValue());
						}
						usersItemsMap.clear();
					}

					synchronized(webpagesSharesMap) {
						logger.info(webpagesSharesMap.size() + " web pages");
						for(Entry<String, Integer> e : webpagesSharesMap.entrySet()) {
							webPageDAO.updateWebPageShares(e.getKey(), e.getValue());
						}
						webpagesSharesMap.clear();
					}
					
					t = System.currentTimeMillis() - t;
					logger.info("Mongo Updates took " + t + " milliseconds");
					logger.info("======================================");
					
				} catch (Exception e) {
					if(stop) {
						logger.info("Mongo updater thread interrupted from sleep to stop");
					}
					else {
						logger.error("Exception in mongo updater thread. ", e);
					}
					continue;
				}
			}
		}
		
		public void stopThread() {
			this.stop = true;
			try {
				this.interrupt();
			}
			catch(Exception e) {
				logger.error(e);
			}
		}
	}
}
