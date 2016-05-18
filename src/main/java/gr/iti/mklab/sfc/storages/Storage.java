package gr.iti.mklab.sfc.storages;

import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.ItemState;

import java.io.IOException;


/**
 * Represents a storage for stream items
 *
 */
public interface Storage {

	public boolean open();
	
	public void store(Item update) throws IOException;
	
	public boolean delete(String id) throws IOException;
	
	public boolean checkStatus();

	public void close();
	
	public String getStorageName();

	public void store(ItemState itemState);
	
}
