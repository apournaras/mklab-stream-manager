package eu.socialsensor.sfc.streams.management;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.streams.StreamException;
import eu.socialsensor.framework.streams.StreamHandler;
import eu.socialsensor.sfc.streams.FilterConfiguration;
import eu.socialsensor.sfc.streams.ProcessorConfiguration;
import eu.socialsensor.sfc.streams.StorageConfiguration;
import eu.socialsensor.sfc.streams.StreamsManagerConfiguration;
import eu.socialsensor.sfc.streams.filters.ItemFilter;
import eu.socialsensor.sfc.streams.processors.Processor;
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
	
	private BlockingQueue<Item> queue = new LinkedBlockingDeque<Item>();
	
	private StreamsManagerConfiguration config;
	private Integer numberOfConsumers = 8;
	
	private List<Consumer> consumers;
	
	private List<StreamUpdateStorage> workingStorages = new ArrayList<StreamUpdateStorage>();
	
	private Map<String, ItemFilter> filtersMap = new HashMap<String, ItemFilter>();
	private Map<String, Processor> processorsMap = new HashMap<String, Processor>();
	
	private StorageStatusAgent statusAgent;
	
	private Map<String,Boolean> workingStatus = new HashMap<String, Boolean>();
	private int items = 0;
	
	enum StoreManagerState {
		OPEN, CLOSE
	}
	
	private StoreManagerState state = StoreManagerState.CLOSE;
	
	public StoreManager(StreamsManagerConfiguration config) {
		
		state = StoreManagerState.OPEN;
		this.config = config;
		
		consumers = new ArrayList<Consumer>(numberOfConsumers);
		
		try {
			createFilters();
			logger.info(filtersMap.size() + " filters initialized!");
			
			createProcessors();
			logger.info(filtersMap.size() + " processors initialized!");
			
			store = initStorage(config);	
		} catch (StreamException e) {
			e.printStackTrace();
			logger.error(e);
		}
		
		this.statusAgent = new StorageStatusAgent(this);
		this.statusAgent.start();
		
	}
	
	public Map<String,Boolean> getWorkingDataBases() {
		return workingStatus;
	}
	
	public void updateDataBasesStatus(String storageId,boolean status){
		workingStatus.put(storageId, status);
	}
	
	public StoreManagerState getState() {
		return state;
	}
	
	public MultipleStorages getStorages() {
		return store;
	}
	
	public void eliminateStorage(StreamUpdateStorage storage) {
		store.remove(storage);
	}
	
	public void restoreStorage(StreamUpdateStorage storage) {
		store.register(storage);
	}
	
	/**
	 * Initiates the consumer threads that are responsible for storing
	 * to the database.
	 */
	public void start() {
		
		for(int i=0; i<numberOfConsumers; i++)
			consumers.add(new Consumer(queue, store, filtersMap.values(), processorsMap.values()));
		
		for(Consumer consumer : consumers)
			consumer.start();
	}
	
	//StreamHandler methods
	@Override
	public void error(StreamError error) {
		logger.error(error.getException());
		error.getException().printStackTrace();
	}

	
	@Override
	public void update(Item item) {
		//synchronized(queue) {
			try {
				items++;
				queue.add(item);
			}
			catch(Exception e) {
				logger.error(e);
			}
			
		//}	
	}

	@Override
	public void updates(Item[] items) {
		for (Item item : items) {
			update(item);
		}
	}
	
	
	@Override
	public void delete(Item item) {
		//synchronized(queue) {
			queue.add(item);
		//}		
	}
	
	
	public void deleteItemsOlderThan(long dateThreshold){
		try {
			store.deleteItemsOlderThan(dateThreshold);
		} catch (IOException e) {
			logger.error(e);
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
			
			if(storage.open(storageInstance)) {
				workingStatus.put(storageId, true);
				storage.register(storageInstance);
			}
			else
				workingStatus.put(storageId, false);	
			
		}
		
		return storage;
	}
	
	private void createFilters() throws StreamException {
		try {
			for (String filterId : config.getFilterIds()) {
				
				FilterConfiguration fconfig = config.getFilterConfig(filterId);
				String className = fconfig.getParameter(FilterConfiguration.CLASS_PATH);
				Constructor<?> constructor = Class.forName(className).getConstructor(FilterConfiguration.class);
				ItemFilter filterInstance = (ItemFilter) constructor.newInstance(fconfig);
			
				filtersMap.put(filterId, filterInstance);
			}
		}catch(Exception e) {
			e.printStackTrace();
			throw new StreamException("Error during streams initialization", e);
		}
	}
	
	private void createProcessors() throws StreamException {
		try {
			for (String processorId : config.getProcessorsIds()) {
				
				ProcessorConfiguration pconfig = config.getProcessorConfig(processorId);
				String className = pconfig.getParameter(ProcessorConfiguration.CLASS_PATH);
				Constructor<?> constructor = Class.forName(className).getConstructor(ProcessorConfiguration.class);
				Processor processorInstance = (Processor) constructor.newInstance(pconfig);
			
				processorsMap.put(processorId, processorInstance);
			}
		}catch(Exception e) {
			e.printStackTrace();
			throw new StreamException("Error during streams initialization", e);
		}
	}
	
	/**
	 * Stops all consumer threads and all the databases used
	 */
	public void stop() {
		for(Consumer consumer : consumers) {
			consumer.die();
		}
		store.close();
		
		state = StoreManagerState.CLOSE;
		do {
			statusAgent.interrupt();
		}
		while(statusAgent.isAlive());
	}
	
	/**
	 * Re-initializes all databases in case of error response
	 * @throws StreamException
	 */
	public void reset() throws StreamException {
		System.out.println("Try to connect to server again - Reinitialization.... ");
		if (this != null) {
			this.stop();
		}
		
		this.store = initStorage(config);
		logger.info("Dumper has started - I can store items again!");
	}
	
	public class StorageStatusAgent extends Thread {
		// Runs every two minutes by default
		private long minuteThreshold = 2 * 60000;
		
		private StoreManager storeManager;
		
		public StorageStatusAgent(StoreManager storeManager) {
			this.storeManager = storeManager;
			logger.info("Status Check Thread initialized");
		}
		
		public void run() {
			int p = items;
			long T0 = System.currentTimeMillis();
			long T = System.currentTimeMillis();
			
			while(storeManager.getState().equals(StoreManagerState.OPEN)) {
				
				try {
					Thread.sleep(minuteThreshold);
				} catch (InterruptedException e) {
					if(storeManager.getState().equals(StoreManagerState.CLOSE)) {
						logger.info("StorageStatusAgent interrupted from sleep to stop.");
						break;
					}
					else {
						logger.error("Exception in StorageStatusAgent. ", e);
					}
				}
				
				for(StreamUpdateStorage storage : workingStorages) {
					String storageId = storage.getStorageName();
					Boolean status = store.checkStatus(storage);
					
					if(!status && storeManager.getWorkingDataBases().get(storageId)){     
						//was working and now is not responding
						logger.info(storageId + " was working and now is not responding");
						storeManager.updateDataBasesStatus(storageId, status);
						storeManager.eliminateStorage(storage);
					}
					else if(status && !storeManager.getWorkingDataBases().get(storageId)){
						//was not working and now is working
						logger.info(storageId + " was not working and now is working");
						storeManager.updateDataBasesStatus(storageId, status);
						storeManager.restoreStorage(storage);
					}
				}
				
				// Print handle rates
				long T1 = System.currentTimeMillis();
				logger.info("Queue size: " + queue.size());
				logger.info("Handle rate: " + (items-p)/((T1-T)/60000) + " items/min");
				logger.info("Mean handle rate: " + (items)/((T1-T0)/60000) + " items/min");
				logger.info("============================================================");
				T = System.currentTimeMillis();
				p = items;
				
				// This should never happen
				if(queue.size() > 500) {
					//synchronized(queue) {
						logger.info("Queue size > 500. Clear queue to prevent heap overflow.");
						queue.clear();
					//}
				}
			}
		
		}	
	}
}