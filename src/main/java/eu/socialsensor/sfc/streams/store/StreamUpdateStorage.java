package eu.socialsensor.sfc.streams.store;

import eu.socialsensor.framework.common.domain.Item;

import java.io.IOException;


/**
 * Represents a storage for stream items
 *
 */
public interface StreamUpdateStorage {

	/**
	 * Opens the store
	 * @throws IOException
	 */
	public void open() throws IOException;
	
	/**
	 * Stores a single stream update within store
	 * @param update
	 * 			Stream update to store
	 * @throws IOException
	 */
	public void store(Item update) throws IOException;
	
	/**
	 * Stores a single stream update within store
	 * @param update
	 * 			Stream update to store
	 * @throws IOException
	 */
	public void update(Item update) throws IOException;
	
	/**
	 * Delete a single stream update within store based on its id
	 * @param update
	 * 			Stream update to delete
	 * @return
	 *         Deletion status
	 * @throws IOException
	 */
	public boolean delete(String id) throws IOException;
	
	/**
	 * Update timeslot
	 */
	public void updateTimeslot();
	
	/**
	 * Close the store
	 */
	public void close();
	
}
