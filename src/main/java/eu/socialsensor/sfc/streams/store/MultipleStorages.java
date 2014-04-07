package eu.socialsensor.sfc.streams.store;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.sfc.streams.StorageConfiguration;
import eu.socialsensor.sfc.streams.StreamsManagerConfiguration;

/**
 * Class for handling store actions for different types of storages
 * (mongoDB, solr, flatfile, redis, lucene ect)
 * @author manosetro
 * @email  manosetro@iti.gr
 */
public class MultipleStorages implements StreamUpdateStorage {
	
	List<StreamUpdateStorage> storages = new ArrayList<StreamUpdateStorage>();
	
	
	public MultipleStorages() {
		
	}
	
	public MultipleStorages(StreamsManagerConfiguration config) {
		for (String storageId : config.getStorageIds()) {
			StorageConfiguration storage_config = config.getStorageConfig(storageId);
			StreamUpdateStorage storage_instance;
			try {
				String storageClass = storage_config.getParameter(StorageConfiguration.CLASS_PATH);
				Constructor<?> constructor
					= Class.forName(storageClass).getConstructor(StorageConfiguration.class);
				storage_instance = (StreamUpdateStorage) constructor.newInstance(storage_config);
				
			} catch (Exception e) {
				return;
			}
			
			this.register(storage_instance);
		}
	}
	
	@Override
	public boolean open(){
		synchronized(storages) {
			for(StreamUpdateStorage storage : storages) {
				storage.open();
			}
		}
		return true;
	}
	
	public boolean open(StreamUpdateStorage storage){
		return storage.open();
	}
	
	@Override
	public void store(Item update) throws IOException {
		synchronized(storages) {
			for(StreamUpdateStorage storage : storages) {
				storage.store(update);
			}
		}
	}
	
	@Override
	public void update(Item update) throws IOException {
		synchronized(storages) {
			for(StreamUpdateStorage storage : storages) {
				storage.update(update);
			}
		}
	}
	
	@Override
	public boolean delete(String id) throws IOException {
		synchronized(storages) {
			boolean deleted = true;
			for(StreamUpdateStorage storage : storages) {
				deleted = deleted && storage.delete(id);
			}
			return deleted;
		}
	}

	@Override
	public void close() {
		for(StreamUpdateStorage storage : storages) {
			storage.close();
		}
		storages.clear();
	}

	public void register(StreamUpdateStorage storage) {
		System.out.println("Register storage "+storage.getStorageName());
		storages.add(storage);
	}
	
	public void remove(StreamUpdateStorage storage){
		System.out.println("Remove storage "+storage.getStorageName());
		storages.remove(storage);
	}
	
	public List<StreamUpdateStorage> getRegisteredStorages(){
		return storages;
	}

	@Override
	public void updateTimeslot() {
		synchronized(storages) {
			for(StreamUpdateStorage storage : storages) {
				storage.updateTimeslot();
			}
		}
	}

	@Override
	public boolean checkStatus(StreamUpdateStorage storage) {
		return storage.checkStatus(storage);
	}
	
	@Override
	public String getStorageName(){
		return null;
	}
}
