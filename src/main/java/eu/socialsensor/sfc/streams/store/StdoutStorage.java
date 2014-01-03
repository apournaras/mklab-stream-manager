
package eu.socialsensor.sfc.streams.store;

import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.sfc.streams.StorageConfiguration;

import java.io.IOException;

public class StdoutStorage implements StreamUpdateStorage {

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
	public void open() throws IOException {
		
	}


	@Override
	public void close() {
		
	}
	

	@Override
	public void updateTimeslot() {
	}


	@Override
	public void update(Item update) throws IOException {
		System.out.println(update.toJSONString());
	}

}
