package gr.iti.mklab.sfc;

import java.util.concurrent.BlockingQueue;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mongodb.morphia.Morphia;

import gr.iti.mklab.framework.common.domain.collections.Collection;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

public class RedisSubscriber extends JedisPubSub implements Runnable {

		private Logger logger = LogManager.getLogger(RedisSubscriber.class);
		private Morphia morphia = new Morphia();
		
		private BlockingQueue<Pair<Collection, String>> queue;
		
		private Jedis jedis;
		
		public RedisSubscriber(BlockingQueue<Pair<Collection, String>> queue, Jedis jedis) {
			this.jedis = jedis;
			this.queue = queue;
			this.morphia.map(Collection.class);
		}
		
	    @Override
	    public void onMessage(String channel, String message) {
	    	logger.info("Channel: " + channel + ", Message: " + message);
	    }

	    @Override
	    public void onPMessage(String pattern, String channel, String message) {	
	    	try {
	    		logger.info("Pattern: " + pattern + ", Channel: " + channel + ", Message: " + message);
	    		
	    		DBObject obj = (DBObject) JSON.parse(message);	    	
	    		Collection collection = morphia.fromDBObject(Collection.class, obj);
	    		
	    		Pair<Collection, String> actionPair = Pair.of(collection, channel);
				queue.put(actionPair);
				
	    	}
	    	catch(Exception e) {
	    		logger.error("Error on message " + message + ". Channel: " + channel + " pattern: " + pattern, e);
	    	}
	    }

	    @Override
	    public void onSubscribe(String channel, int subscribedChannels) {
	    	logger.info("Subscribe to " + channel + " channel. " + subscribedChannels + " active subscriptions.");
	    }

	    @Override
	    public void onUnsubscribe(String channel, int subscribedChannels) {
	    	logger.info("Unsubscribe from " + channel + " channel. " + subscribedChannels + " active subscriptions.");
	    }

	    @Override
	    public void onPUnsubscribe(String pattern, int subscribedChannels) {
	    	logger.info("Unsubscribe to " + pattern + " pattern. " + subscribedChannels + " active subscriptions.");
	    }

	    @Override
	    public void onPSubscribe(String pattern, int subscribedChannels) {
	    	logger.info("Subscribe from " + pattern + " pattern. " + subscribedChannels + " active subscriptions.");
	    }

		@Override
		public void run() {
			logger.info("Subscribe to channel collections:*");
			jedis.psubscribe(this, "collections:*");
			
			logger.info("Subscriber shutdown");
		}
	}