package gr.iti.mklab.sfc.storages;

import java.io.IOException;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.ItemState;
import gr.iti.mklab.framework.common.domain.MediaItem;
import gr.iti.mklab.framework.common.domain.WebPage;

/**
 * Class for storing items to redis store
 * 
 * @author manosetro - manosetro@iti.gr
 *
 */
public class RedisStorage implements Storage {

	private static String HOST = "redis.host";
	
	private static String WEBPAGES_CHANNEL = "redis.webpages.channel";
	private static String MEDIA_CHANNEL = "redis.media.channel";
	private static String ITEMS_CHANNEL = "redis.items.channel";
	
	private Logger  logger = LogManager.getLogger(RedisStorage.class);
	
	private JedisPool jedisPool;
	private Jedis jedis;
	private String host;
	
	private String itemsChannel = null;
	private String webPagesChannel = null;
	private String mediaItemsChannel = null;
	
	private long items = 0, mItems = 0, wPages = 0;
	
	private String storageName = "Redis";
	
	public RedisStorage(Configuration config) {
		this.host = config.getParameter(RedisStorage.HOST);
		this.itemsChannel = config.getParameter(RedisStorage.ITEMS_CHANNEL);
		this.webPagesChannel = config.getParameter(RedisStorage.WEBPAGES_CHANNEL);
		this.mediaItemsChannel = config.getParameter(RedisStorage.MEDIA_CHANNEL);
	}
	
	@Override
	public boolean open() {
		try {
			JedisPoolConfig poolConfig = new JedisPoolConfig();
			this.jedisPool = new JedisPool(poolConfig, host, 6379, 0);
			this.jedis = jedisPool.getResource();
			return true;
		}
		catch(Exception e) {
			logger.error("Error during opening.", e);
			return false;
		}
	}

	@Override
	public void store(Item item) throws IOException {
		
		if(item == null)
			return;
		
		if(item.isOriginal()) { 	
			if(itemsChannel != null) {
				items++;
				synchronized(jedis) {
					jedis.publish(itemsChannel, item.toString());
				}
			}
		
			if(mediaItemsChannel != null) {
				for(MediaItem mediaItem : item.getMediaItems()) {
					mItems++;
					synchronized(jedis) {
						jedis.publish(mediaItemsChannel, mediaItem.toString());
					}
				}
			}
		
			if(webPagesChannel != null) {
				for(WebPage webPage : item.getWebPages()) {
					wPages++;
					synchronized(jedis) {
						jedis.publish(webPagesChannel, webPage.toString());
					}
				}
			}
		}
	}
	
	@Override
	public void store(ItemState itemState) {
		System.out.println(itemState.toString());	
	}
	
	@Override
	public boolean delete(String id) throws IOException {
		// Not supported.
		return false;
	}


	@Override
	public void close() {
		jedis.disconnect();
		jedisPool.close();
	}
	
	@Override
	public boolean checkStatus() {
		try {
			logger.info("Redis sent " + items + " items, " + mItems + " media items and " 
					+ wPages + " web pages!");

			boolean connected;
			synchronized(jedis) {
				jedis.info();
				connected = jedis.isConnected();
			}
			if(!connected) {
				connected = reconnect();
			}
			return connected;
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error(e);
			return reconnect();
		}
	}
	
	private boolean reconnect() {
		synchronized(jedis) {
			try {
				if(jedis != null) {
					jedis.disconnect();
				}
			}
			catch(Exception e) { 
				logger.error(e);
			}
		}
		try {
			synchronized(jedis) {        	
        		this.jedis = jedisPool.getResource();
        		jedis.info();
        		return jedis.isConnected();
			}
		}
		catch(Exception e) {
			logger.error(e);
			return false;
		}

	}
	
	@Override
	public String getStorageName() {
		return this.storageName;
	}

}
