package gr.iti.mklab.sfc.storages;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.query.Query;

import com.mongodb.MongoException;
import com.mongodb.WriteResult;

import gr.iti.mklab.framework.client.mongo.DAOFactory;
import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.MediaItem;
import gr.iti.mklab.framework.common.domain.StreamUser;
import gr.iti.mklab.framework.common.domain.WebPage;

/**
 * Class for storing items in mongo db
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 *
 */
public class MongoDbStorage implements Storage {

	private static String HOST = "mongodb.host";
	private static String DB = "mongodb.database";
	
	private Logger logger = Logger.getLogger(MongoDbStorage.class);
	
	private String storageName = "Mongodb";
	
	private String host;
	private String database;
	
	private BasicDAO<Item, String> itemDAO = null;
	private BasicDAO<MediaItem, String> mediaItemDAO = null;
	private BasicDAO<StreamUser, String> streamUserDAO = null;
	private BasicDAO<WebPage, String> webPageDAO = null;
	
	private Integer items = 0, mediaItems = 0, wPages = 0, users = 0;
	private Integer itemInsertions = 0, mediaItemInsertions = 0, wPageInsertions = 0, userInsertions = 0;
	
	private HashMap<String, Integer> webpagesSharesMap;
	private HashMap<String, Integer> mediaItemsSharesMap;
	private HashMap<String, Item> itemsMap;
	private HashMap<String, StreamUser> usersMap;
	
	public MongoDbStorage(Configuration config) {	
		this.host = config.getParameter(MongoDbStorage.HOST);
		this.database = config.getParameter(MongoDbStorage.DB);
	
		this.itemsMap = new HashMap<String, Item>();
		this.usersMap = new HashMap<String, StreamUser>();
		this.webpagesSharesMap = new HashMap<String, Integer>();
		this.mediaItemsSharesMap = new HashMap<String, Integer>();
	}
	
	@Override
	public void close() {
		
	}

	@Override
	public boolean delete(String id) throws IOException {
		WriteResult result = itemDAO.deleteById(id);
		if(result.getN() > 0) {
			return true;
		}
		
		return false;
	}
	
	@Override
	public boolean open() {
		
		logger.info("Open MongoDB storage <host: " + host + ">");
		
		DAOFactory daoFactory = new DAOFactory();
		if(database != null) {
			try {
				itemDAO = daoFactory.getDAO(host, database, Item.class);
				mediaItemDAO = daoFactory.getDAO(host, database, MediaItem.class);
				streamUserDAO = daoFactory.getDAO(host, database, StreamUser.class);
				webPageDAO = daoFactory.getDAO(host, database, WebPage.class);
				
			} catch (Exception e) {
				logger.error("MongoDB Storage failed to open!");
				logger.error(e);
				e.printStackTrace();
				return false;
			}
		}
		
		return true;
	}

	
	@Override
	public void store(Item item) {
		
		try {
			// Handle Items
			String itemId = item.getId();
			
			boolean itemExists = false;
			synchronized(itemsMap) {
				Query<Item> q = itemDAO.createQuery().filter("id", itemId);
				itemExists = itemsMap.containsKey(itemId) || itemDAO.exists(q);
			}
			
			items++;
			if(!itemExists) {
				
				// Item does not exist in MongoDB. Save it.
				itemInsertions++;
				
				item.setInsertionTime(System.currentTimeMillis());
				itemDAO.save(item);
				
				if(item.getMentions() != null) {
					String[] mentionedUsers = item.getMentions();
					for(String mentionedUser : mentionedUsers) {
						synchronized(usersMap) {
							StreamUser tempUser = usersMap.get(mentionedUser);
							if(tempUser == null) {
								tempUser = new StreamUser();
								tempUser.setId(mentionedUser);
								usersMap.put(mentionedUser, tempUser);
							}
							tempUser.incMentions(1L);
						}
					}
				}

				if(item.getReferencedUserId() != null) {
					String userid = item.getReferencedUserId();
					
					synchronized(usersMap) {
						StreamUser tempUser = usersMap.get(userid);
						if(tempUser == null) {
							tempUser = new StreamUser();
							tempUser.setId(userid);
							usersMap.put(userid, tempUser);
						}
						tempUser.incShares(1L);
					}
				}
				
				// Handle Media Items
				if(mediaItemDAO != null && item.getMediaItems() != null) {
					for(MediaItem mediaItem : item.getMediaItems()) {
						mediaItems++;
						String mediaItemId = mediaItem.getId();
					
						boolean mediaExists = false;
						synchronized(mediaItemsSharesMap) {
							mediaExists = mediaItemsSharesMap.containsKey(mediaItemId) || mediaItemDAO.exists(mediaItemDAO.createQuery().filter("id", mediaItemId));
						}
					
						if(!mediaExists) {	
							// MediaItem does not exist. Save it.
							mediaItemInsertions++;
							mediaItemDAO.save(mediaItem);
							synchronized(mediaItemsSharesMap) {
								mediaItemsSharesMap.put(mediaItemId, 0);
							}
						}
						else {
							//Update media item
							synchronized(mediaItemsSharesMap) {
								Integer shares = mediaItemsSharesMap.get(mediaItemId);
								if(shares == null) {
									shares = 0;
								}
								mediaItemsSharesMap.put(mediaItemId, ++shares);
							}
						}
					}
				}
				
				// Handle Web Pages
				List<WebPage> webPages = item.getWebPages();
				if(webPages != null && webPageDAO != null) {
					for(WebPage webPage : webPages) {
						wPages++;
						String webPageURL = webPage.getUrl();
						
						boolean wpExists = false;
						synchronized(webpagesSharesMap) {
							wpExists = webpagesSharesMap.containsKey(webPageURL) || webPageDAO.exists(webPageDAO.createQuery().filter("url", webPageURL));
						}
						
						if(!wpExists) {
							// Web page does not exist. Save it.
							wPageInsertions++;
							webPageDAO.save(webPage);
							synchronized(webpagesSharesMap) {
								webpagesSharesMap.put(webPageURL, 1);
							}
						}
						else {
							synchronized(webpagesSharesMap) {
								Integer shares = webpagesSharesMap.get(webPageURL);
								if(shares == null) {
									shares = 0;
								}
								webpagesSharesMap.put(webPageURL, ++shares);
							}
						}
					}
				}
			}
			
			// Handle Stream Users
			StreamUser user = item.getStreamUser();
			if(user != null) {
				
				user.setLastUpdated(System.currentTimeMillis());
				
				users++;
				
				String userId = user.getId();
				boolean userExists = false;
				synchronized(usersMap) {
					userExists = usersMap.containsKey(userId);
				}
				
				if(!userExists) {
					// save stream user
					userInsertions++;
					streamUserDAO.save(user);
					
					StreamUser tempUser = usersMap.get(user.getId());
					if(tempUser == null) {
						tempUser = new StreamUser();
						tempUser.setId(user.getId());
						tempUser.setProfileImage(user.getProfileImage());
						tempUser.setName(user.getName());
						tempUser.setLastUpdated(user.getLastUpdated());
						usersMap.put(user.getId(), tempUser);
					}
					tempUser.incItems(1);
					tempUser.incMentions(1L);
				}
			}
			
		}
		catch(MongoException e) {
			e.printStackTrace();
			logger.error("Storing item " + item.getId() + " failed.");
		}
	
	}

	@Override
	public void update(Item update) throws IOException {
		// update item
		store(update);
	}
	
	@Override
	public boolean checkStatus() {
		return true;
	}
	
	@Override
	public String getStorageName() {
		return this.storageName;
	}
	
	/*
	private class UpdaterTask extends Thread {

		private long timeout = 10 * 60 * 1000;
		private boolean stop = true;
		
		@Override
		public void run() {
			stop = false;
			while(!stop) {
				try {
					
					synchronized(this) {
						this.wait(timeout);
					}
					
					logger.info("Update: ");
					long t = System.currentTimeMillis();
					
					synchronized(itemsMap) {
						logger.info(itemsMap.size() + " items to update");
						for(Item item : itemsMap.values()) {
							//itemDAO.updateItem(item);
						}
						itemsMap.clear();
					}
					
					synchronized(usersMap) {
						logger.info(usersMap.size() + " users to update");
						for(Entry<String, StreamUser> user : usersMap.entrySet()) {
							//streamUserDAO.updateStreamUserStatistics(user.getValue());
						}
						usersMap.clear();
					}
					
					if(webPageDAO != null) {
						synchronized(webpagesSharesMap) {
							logger.info(webpagesSharesMap.size() + " web pages to update");
							for(Entry<String, Integer> e : webpagesSharesMap.entrySet()) {
								//webPageDAO.updateWebPageShares(e.getKey(), e.getValue());
							}
							webpagesSharesMap.clear();
						}
					}
					
					if(mediaItemDAO != null) {
						synchronized(mediaItemsSharesMap) {
							logger.info(mediaItemsSharesMap.size() + " media Items to update");
							for(Entry<String, Integer> entry : mediaItemsSharesMap.entrySet()) {
								//mediaItemDAO.updateMediaItemShares(entry.getKey(), entry.getValue());
							}
							mediaItemsSharesMap.clear();
						}
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
						logger.info(mediaItemsSharesMap.size() + " media Items to update");
						logger.info(webpagesSharesMap.size() + " web pages to update");
						logger.info(usersMap.size() + " users to update");
						logger.info(itemsMap.size() + " items to update");
						
						mediaItemsSharesMap.clear();
						webpagesSharesMap.clear();
						usersMap.clear();
						itemsMap.clear();
						
					}
					continue;
				}
			}
		}
		
		public void stopTask() {
			logger.info("Stop updater task");
			try {
				this.stop = true;
				this.interrupt();
			}
			catch(Exception e) {
				logger.error("Fail to stop update task in MongoDBStorage", e);
			}
		}
		
	}
	*/
	
}
