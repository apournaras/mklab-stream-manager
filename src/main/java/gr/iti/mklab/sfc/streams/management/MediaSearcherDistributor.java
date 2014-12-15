package gr.iti.mklab.sfc.streams.management;

import gr.iti.mklab.framework.common.domain.Message;
import gr.iti.mklab.framework.common.domain.Message.Action;
import gr.iti.mklab.framework.common.factories.ObjectFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

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
        
        ExecutorService executor = Executors.newFixedThreadPool(1);
        executor.submit(new RequestSender(queue, jedisPool, channel, n));
        
        System.out.println("Subscribe to redis");
        Listener receiver = new Listener(queue);
        jedis.subscribe(receiver, channel);

        logger.info("Media Searcher Distributor initialized.");
        
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
		private JedisPool _jedisPool;

		public RequestSender(BlockingQueue<String> queue, JedisPool jedisPool, String channel, int n) {
			_queue = queue;
			_jedisPool = jedisPool;
			_jedis = _jedisPool.getResource();
			_channel = channel;
			_n = n;
		}
		
		@Override
        public void run() {
			int i = 1;
			while(true) {
				try {
					String msg = _queue.take();
					Message dyscoMessage = ObjectFactory.createMessage(msg);
					
					boolean isConnected = true;
					try {
						_jedis.ping();
					}
					catch(Exception e) {
						logger.error("Client is not connected");
						isConnected = false;
					}
					
					if(!isConnected) {
						try {
							_jedisPool.returnBrokenResource(_jedis);
						}
						catch(Exception e) {
							logger.error("Error while returning broken resource");
						}
						_jedis = _jedisPool.getResource();
					}
					
					if(dyscoMessage.getAction().equals(Action.DELETE)) {
						logger.info("Send DELETE message to all channels");
						if(msg != null) {
							
							_jedis.publish(_channel+"_*", msg);	
						}
						continue;
					}
					
					i = i%_n + 1;
					String channel = _channel + "_" + i;
					logger.info("Send message to " + channel);
					if(msg != null) {
						_jedis.publish(channel, msg);	
					}
					
					
				} catch (Exception e) {
					e.printStackTrace();
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
	public static class Listener extends JedisPubSub {

		private BlockingQueue<String> _queue;

		public Listener(BlockingQueue<String> queue) {
			_queue = queue;
		}

	    @Override
	    public void onMessage(String channel, String message) {	    	
	    	logger.info("Receive " + message);
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
	
}
