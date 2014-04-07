<<<<<<< HEAD
package eu.socialsensor.sfc.streams.management;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;




import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.streams.StreamException;
import eu.socialsensor.framework.streams.StreamHandler;
import eu.socialsensor.sfc.streams.StorageConfiguration;
import eu.socialsensor.sfc.streams.StreamsManagerConfiguration;
import eu.socialsensor.sfc.streams.store.Consumer;
import eu.socialsensor.sfc.streams.store.MultipleStorages;
import eu.socialsensor.sfc.streams.store.StreamUpdateStorage;
import eu.socialsensor.framework.streams.StreamError;

/**
 * @brief  Thread-safe class for managing the storage of items to databases 
 * The storage may be accomplished using multiple consumer-threads.
 * 
 * @author ailiakop
 * @email  ailiakop@iti.gr
 *
 */
public class StoreManager implements StreamHandler {
	
	private MultipleStorages store = null;
	private Queue<Item> queue = new ArrayDeque<Item>();
	private StreamsManagerConfiguration config;
	private Integer numberOfConsumers = 1;
	private List<Consumer> consumers;
	private List<StreamUpdateStorage> workingStorages = new ArrayList<StreamUpdateStorage>();
	
	private StorageStatusAgent statusAgent;
	
	private Map<String,Boolean> workingStatus = new HashMap<String,Boolean>();
	private int items = 0;
	
	enum StoreManagerState {
		OPEN, CLOSE
	}
	private StoreManagerState state = StoreManagerState.CLOSE;
	
	public StoreManager(StreamsManagerConfiguration config) {
		
		state = StoreManagerState.OPEN;
		this.config = config;
		
		try {
			store = initStorage(config);
		} catch (StreamException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		this.statusAgent = new StorageStatusAgent(this);
		this.statusAgent.start();
	}
	
	public StoreManager(StreamsManagerConfiguration config, Integer numberOfConsumers) throws IOException {
	
		this.config = config;
		this.numberOfConsumers = numberOfConsumers;
		
		try {
			store = initStorage(config);
		} catch (StreamException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		this.statusAgent = new StorageStatusAgent(this);
		this.statusAgent.start();
	}
	
	public Map<String,Boolean> getWorkingDataBases(){
		return workingStatus;
	}
	
	public void updateDataBasesStatus(String storageId,boolean status){
		workingStatus.put(storageId, status);
	}
	
	public StoreManagerState getState(){
		return state;
	}
	
	public MultipleStorages getStorages(){
		return store;
	}
	
	public void eliminateStorage(StreamUpdateStorage storage){
		store.remove(storage);
	}
	
	public void restoreStorage(StreamUpdateStorage storage){
		store.register(storage);
	}
	
	/**
	 * Initiates the consumer threads that are responsible for storing
	 * to the database.
	 */
	public void start() {
		
		Thread thread = new Thread(new Statistics());
		thread.start();
		
		consumers = new ArrayList<Consumer>(numberOfConsumers);
		
		for(int i=0;i<numberOfConsumers;i++)
			consumers.add(new Consumer(queue, store));
		
		for(Consumer consumer : consumers)
			consumer.start();
	}
	
	//StreamHandler methods
	
	@Override
	public void error(StreamError error) {
		System.err.println(error.getMessage());
		error.getException().printStackTrace();
	}

	
	@Override
	public void update(Item item) {
		
		synchronized(queue) {
			items++;
			queue.add(item);
		}	
	
	}

	@Override
	public void updates(Item[] items) {
		for (Item item : items) {
			update(item);
		}
	}
	
	
	@Override
	public void delete(Item item) {
		synchronized(queue) {
			queue.add(item);
		}		
	}
	
	
	/**
	 * Initializes the databases that are going to be used in the service
	 * @param config
	 * @return
	 * @throws StreamException
	 */
	private MultipleStorages initStorage(StreamsManagerConfiguration config) throws StreamException {
		MultipleStorages storage = new MultipleStorages();
		
		for (String storageId : config.getStorageIds()) {
			
			StorageConfiguration storageConfig = config.getStorageConfig(storageId);
			StreamUpdateStorage storageInstance;
			try {
				String storageClass = storageConfig.getParameter(StorageConfiguration.CLASS_PATH);
				Constructor<?> constructor
					= Class.forName(storageClass).getConstructor(StorageConfiguration.class);
				storageInstance = (StreamUpdateStorage) constructor.newInstance(storageConfig);
				workingStorages.add(storageInstance);
			} catch (Exception e) {
				throw new StreamException("Error during storage initialization", e);
			}
			
			if(storage.open(storageInstance)){
				workingStatus.put(storageId, true);
				storage.register(storageInstance);
			}
			else
				workingStatus.put(storageId, false);	
			
		}
		
		return storage;
	}
	
	/**
	 * Stops all consumer threads and all the databases used
	 */
	public void stop(){
		for(Consumer consumer : consumers){
			consumer.die();
		}
		
		store.close();
		
		state = StoreManagerState.CLOSE;
	}
	
	/**
	 * Re-initializes all databases in case of error response
	 * @throws StreamException
	 */
	public void reset() throws StreamException{
		System.out.println("Try to connect to server again - Reinitialization.... ");
		if (this != null) {
			this.stop();
		}
		
		this.store = initStorage(config);
		
		logger.info("Dumper has started - I can store items again!");
	}
	
	public class StorageStatusAgent extends Thread {
		private long minuteThreshold = 60000;
		private long currentTime,periodTime; 
		private StoreManager storeManager;
		
		public StorageStatusAgent(StoreManager storeManager){
			this.storeManager = storeManager;
			logger.info("Status Check Thread initialized");
			
		}
		
		public void run(){
			while(storeManager.getState().equals(StoreManagerState.OPEN)){
				periodTime = System.currentTimeMillis();
				currentTime = System.currentTimeMillis();
				while((currentTime - periodTime) < minuteThreshold){
				
					currentTime = System.currentTimeMillis();
				}
				
				for(StreamUpdateStorage storage : workingStorages){
					String storageId = storage.getStorageName();
					Boolean status = store.checkStatus(storage);
					
					if(!status && storeManager.getWorkingDataBases().get(storageId)){     //was working and now is not responding
						storeManager.updateDataBasesStatus(storageId, status);
						storeManager.eliminateStorage(storage);
					}
					else if(status && !storeManager.getWorkingDataBases().get(storageId)){//was not working and now is working
						storeManager.updateDataBasesStatus(storageId, status);
						storeManager.restoreStorage(storage);
					}
				}

			}
		
		}
		
	}
	
	private class Statistics implements Runnable {
		
		@Override
		public void run() {
			int p = items, t = 0;
			while(true) {
				try {
					Thread.sleep(5000);
					logger.info("Queue size: " + queue.size());
					logger.info("Handle rate: " + (items-p)/5 + " items/sec");
					
					t +=5;
					logger.info("Mean handle rate: " + (items)/t + " items/sec");
					p = items;
					
				} catch (InterruptedException e) { }
			}
			
		}
		
	}
}
=======
package eu.socialsensor.sfc.streams.management;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;




import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.streams.StreamException;
import eu.socialsensor.framework.streams.StreamHandler;
import eu.socialsensor.sfc.streams.StorageConfiguration;
import eu.socialsensor.sfc.streams.StreamsManagerConfiguration;
import eu.socialsensor.sfc.streams.store.Consumer;
import eu.socialsensor.sfc.streams.store.MultipleStorages;
import eu.socialsensor.sfc.streams.store.StreamUpdateStorage;
import eu.socialsensor.framework.streams.StreamError;

/**
 * @brief  Thread-safe class for managing the storage of items to databases 
 * The storage may be accomplished using multiple consumer-threads.
 * 
 * @author ailiakop
 * @email  ailiakop@iti.gr
 *
 */
public class StoreManager implements StreamHandler {
	
	private StreamUpdateStorage store = null;
	private Queue<Item> queue = new ArrayDeque<Item>();
	private StreamsManagerConfiguration config;
	private Integer numberOfConsumers = 1;
	private List<Consumer> consumers;
	
	private Map<String,Boolean> workingStatus = new HashMap<String,Boolean>();
	private int items = 0;
	
	public StoreManager(StreamsManagerConfiguration config) {
		super();
	
		this.config = config;
		
		try {
			store = initStorage(config);
		} catch (StreamException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public StoreManager(StreamsManagerConfiguration config, Integer numberOfConsumers) throws IOException {
		super();
	
		this.config = config;
		this.numberOfConsumers = numberOfConsumers;
		
		try {
			store = initStorage(config);
		} catch (StreamException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		
	}
	
	public Map<String,Boolean> getWorkingDataBases(){
		return workingStatus;
	}
	
	/**
	 * Initiates the consumer threads that are responsible for storing
	 * to the database.
	 */
	public void start() {
		
		Thread thread = new Thread(new Statistics());
		thread.start();
		
		consumers = new ArrayList<Consumer>(numberOfConsumers);
		
		for(int i=0;i<numberOfConsumers;i++)
			consumers.add(new Consumer(queue, store));
		
		for(Consumer consumer : consumers)
			consumer.start();
	}
	
	//StreamHandler methods
	
	@Override
	public void error(StreamError error) {
		System.err.println(error.getMessage());
		error.getException().printStackTrace();
	}

	
	@Override
	public void update(Item item) {
		
		synchronized(queue) {
			items++;
			queue.add(item);
		}	
	
	}

	@Override
	public void updates(Item[] items) {
		for (Item item : items) {
			update(item);
		}
	}
	
	
	@Override
	public void delete(Item item) {
		synchronized(queue) {
			queue.add(item);
		}		
	}
	
	
	/**
	 * Initializes the databases that are going to be used in the service
	 * @param config
	 * @return
	 * @throws StreamException
	 */
	private StreamUpdateStorage initStorage(StreamsManagerConfiguration config) throws StreamException {
		MultipleStorages storage = new MultipleStorages();
		
		for (String storageId : config.getStorageIds()) {
			
			StorageConfiguration storageConfig = config.getStorageConfig(storageId);
			StreamUpdateStorage storageInstance;
			try {
				String storageClass = storageConfig.getParameter(StorageConfiguration.CLASS_PATH);
				Constructor<?> constructor
					= Class.forName(storageClass).getConstructor(StorageConfiguration.class);
				storageInstance = (StreamUpdateStorage) constructor.newInstance(storageConfig);
			} catch (Exception e) {
				throw new StreamException("Error during storage initialization", e);
			}
			storage.register(storageInstance);
			
			if(storage.open(storageInstance))
				workingStatus.put(storageId, true);
			else
				workingStatus.put(storageId, false);	
			
		}
		
		return storage;
	}
	
	/**
	 * Stops all consumer threads and all the databases used
	 */
	public void stop(){
		for(Consumer consumer : consumers){
			consumer.die();
		}
		
		store.close();
	}
	
	/**
	 * Re-initializes all databases in case of error response
	 * @throws StreamException
	 */
	public void reset() throws StreamException{
		System.out.println("Try to connect to server again - Reinitialization.... ");
		if (this != null) {
			this.stop();
		}
		
		this.store = initStorage(config);
		
		System.out.println("Dumper has started - I can store items again!");
	}
	
	private class Statistics implements Runnable {
		
		@Override
		public void run() {
			int p = items, t = 0;
			while(true) {
				try {
					Thread.sleep(5000);
					logger.info("Queue size: " + queue.size());
					logger.info("Handle rate: " + (items-p)/5 + " items/sec");
					
					t +=5;
					logger.info("Mean handle rate: " + (items)/t + " items/sec");
					p = items;
					
				} catch (InterruptedException e) { }
			}
			
		}
		
	}
}
>>>>>>> refs/remotes/origin/master
