package gr.iti.mklab.sfc.streams.search;

import gr.iti.mklab.framework.common.domain.Message;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

/**
	 * Class that implements the Redis Client. Receives the DySco request to be served
	 * as a message. The request might entail the creation of a DySco (NEW), the update of
	 * a DySco (UPDATE) or the deletion of a DySco (DELETE). The service finds the received
	 * DySco in Solr database by its id and passes on the request to the Search Handler modules.
	 * @author ailiakop
	 */
	public class DyscoRequestReceiver extends JedisPubSub implements Runnable {
		
		public final Logger logger = Logger.getLogger(DyscoRequestReceiver.class);
	
		private BlockingQueue<Message> messages = new LinkedBlockingQueue<Message>();

		private Thread listener;

		private String redisChannel;
		private Jedis jedisClient = null;
		
		public DyscoRequestReceiver(String redisHost, String redisChannel) {
			
			JedisPoolConfig poolConfig = new JedisPoolConfig();
	        JedisPool jedisPool = new JedisPool(poolConfig, redisHost, 6379, 0);
	        
	        this.jedisClient = jedisPool.getResource();
			this.redisChannel = redisChannel;
			
		}
		
	    @Override
	    public void onMessage(String channel, String message) {
	    	
	    	logger.info("Received dysco request: " + message);
	    	try {
	    		Message dyscoMessage = Message.toObject(message, Message.class);
				messages.put(dyscoMessage);
			} catch (Exception e) {
				logger.error("Receiver exception: " + e.getMessage());
			}
	    	
	    }
	 
	    @Override
	    public void onPMessage(String pattern, String channel, String message) {
	    	// Do Nothing
	    }
	 
	    @Override
	    public void onSubscribe(String channel, int subscribedChannels) {
	    	// Do Nothing
	    }
	 
	    @Override
	    public void onUnsubscribe(String channel, int subscribedChannels) {
	    	// Do Nothing
	    }
	 
	    @Override
	    public void onPUnsubscribe(String pattern, int subscribedChannels) {
	    	// Do Nothing
	    }
	 
	    @Override
	    public void onPSubscribe(String pattern, int subscribedChannels) {
	    	// Do Nothing
	    }

	    public Message getMessage() {
	    	try {
				return messages.poll();
			} catch (Exception e) {
				logger.error(e);
			}
	    	return null;
	    }
	    
		@Override
		public void run() {
			try {          
                logger.info("Subscribe to redis...");
                jedisClient.subscribe(this, redisChannel);  

                logger.info("Subscribe returned, closing down");
                jedisClient.quit();
            } catch (Exception e) {
            	logger.error("Redis Exception: " + e.getMessage());
            }
		}
		
		public void start() {
			this.listener = new Thread(this);
			listener.setName("DyscoRequestReceiver");
			this.listener.start();
		}
		
		public void close() {
			if(jedisClient != null) {
				jedisClient.close();
				logger.info("Jedis Connection is closed");
			}
		}
	}