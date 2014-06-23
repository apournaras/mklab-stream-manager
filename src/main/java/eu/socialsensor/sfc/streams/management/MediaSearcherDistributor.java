package eu.socialsensor.sfc.streams.management;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.dysco.Message;
import eu.socialsensor.framework.common.domain.dysco.Message.Action;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

/**
 * Class responsible for distributing incoming DySco requests from
 * redis to several services for further processing. The number of 
 * services is determined from the arguments in main. The service acts as 
 * a Redis Client that receives DySco messages and distributes them to 
 * various other services. 
 * @author ailiakop
 * @email ailiakop@iti.gr
 */
public class MediaSearcherDistributor {

	public static Logger logger = Logger.getLogger(MediaSearcherDistributor.class);
	
	public static void main(String[] args) {
		
		if(args.length != 3) {
			logger.error("Media Searcher Ditributor takes three parameters as input");
			System.exit(0);
		}
		
		String redisHost = args[0];
		String channel = args[1];
		int n = Integer.parseInt(args[2]);
		
		JedisPoolConfig poolConfig = new JedisPoolConfig();
        JedisPool jedisPool = new JedisPool(poolConfig, redisHost, 6379, 0);
        
        Jedis jedis = jedisPool.getResource();
        
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>();
        
        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.submit(new RequestSender(queue, jedis, channel, n));
        executor.submit(new RequestReceiver(queue, jedisPool, channel), channel);
     
        Runtime.getRuntime().addShutdownHook(new Shutdown(jedis, executor));
        
        logger.info("Media Searcher Ditributor initialized.");
	}

	/**
	 * Class responsible for distributing the requests that receives from Redis
	 * in a sequential order to all services.
	 * @author ailiakop
	 *
	 */
	public static class RequestSender implements Runnable {
		
		private BlockingQueue<String> _queue;
		private Jedis _jedis;
		private String _channel;
		private int _n;

		public RequestSender(BlockingQueue<String> queue, Jedis jedis, String channel, int n) {
			_queue = queue;
			_jedis = jedis;
			_channel = channel;
			_n = n;
		}
		
		@Override
        public void run() {
			int i = 1;
			while(true) {
				try {
					
					String msg = _queue.take();
					
					Message dyscoMessage = Message.create(msg);
					
					if(dyscoMessage.getAction().equals(Action.DELETE)){
						System.out.println("Send DELETE message to all channels");
						if(msg != null) {
							_jedis.publish(_channel+"_1", msg);	
							_jedis.publish(_channel+"_2", msg);	
						}
						continue;
					}
					
					i = i%_n + 1;
					String channel = _channel + "_" + i;
					System.out.println("Send message to " + channel);
					if(msg != null) {
						_jedis.publish(channel, msg);	
					}
					
					
				} catch (Exception e) {
					logger.error(e);
					continue;
				}
				
			}
		}
	}
	
	/**
	 * Class that implements the Redis Client for receiving DySco messages. 
	 * @author ailiakop
	 *
	 */
	public static class RequestReceiver extends JedisPubSub implements Runnable {

		private BlockingQueue<String> _queue;
		private Jedis _jedis;
		private String _channel;

		public RequestReceiver(BlockingQueue<String> queue, JedisPool jedisPool, String channel) {
			_queue = queue;
			_jedis = jedisPool.getResource();
			_channel = channel;
		}
		
		@Override
        public void run() {
            try {
            	System.out.println("Subscribe to redis");
                _jedis.subscribe(this, _channel);
            } catch (Exception e) {
            }
        }

	    @Override
	    public void onMessage(String channel, String message) {	    	
	    	logger.info("Message received: " + message);
	    	_queue.add(message);
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
	}
	
	/**
	 * Class in case system is shutdown. 
	 * Responsible to close all services that are running at the time being
	 * @author ailiakop
	 *
	 */
	private static class Shutdown extends Thread {
		
		private Jedis _jedis;
		private ExecutorService _executor;

		public Shutdown(Jedis jedis, ExecutorService executor) {
			_jedis = jedis;
			_executor = executor;
		}

		public void run() {
			logger.info("Shutting down MediaSearcherDistributor...");
			_executor.shutdownNow();
			_jedis.quit();
			logger.info("Done...");
		}
	}
	
}
