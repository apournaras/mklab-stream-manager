
package eu.socialsensor.sfc.streams.store;

import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.sfc.streams.StorageConfiguration;

import java.io.IOException;

public class StdoutStorage implements StreamUpdateStorage {
	
	private String storageName = "StdOut";

	public StdoutStorage(StorageConfiguration config) {
		
	}


	@Override
	public void store(Item update) throws IOException {
		System.out.println(update.toJSONString());	
	}
	
	

	@Override
	public boolean delete(String id) throws IOException {
		System.out.println("{ delete : " + id + "}");	
		return false;
	}


	@Override
	public boolean open(){
		return true;
	}


	@Override
	public void close() {
		
	}
	

	@Override
	public void updateTimeslot() {
	}

	
	@Override
	public boolean checkStatus(StreamUpdateStorage storage) {
		return true;
	}

	@Override
	public void update(Item update) throws IOException {
		System.out.println(update.toJSONString());
	}
	
	@Override
	public String getStorageName(){
		return this.storageName;
	}

}
