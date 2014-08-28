
package eu.socialsensor.sfc.storages;

import eu.socialsensor.framework.Configuration;
import eu.socialsensor.framework.common.domain.Item;

import java.io.IOException;

public class StdoutStorage implements Storage {
	
	private String storageName = "StdOut";

	public StdoutStorage(Configuration config) {
		
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
	public boolean deleteItemsOlderThan(long dateThreshold) throws IOException{
	
		return true;
	}

	
	@Override
	public boolean checkStatus(Storage storage) {
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
