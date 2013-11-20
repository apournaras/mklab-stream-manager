package eu.socialsensor.sfc.streams.store;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import eu.socialsensor.framework.common.domain.Item;

/**
 * Class for storing items to a flat file
 * @author manosetro
 * @email  manosetro@iti.gr
 *
 */
public class FlatFileStorage implements StreamUpdateStorage {
	
	public static final String STORE_FILE = "/items.";
	
	private File storageDirectory;
	private PrintWriter out = null;
	
	long items = 0;
	
	public FlatFileStorage(String storageDirectory) {
		this.storageDirectory = new File(storageDirectory);
	}
	
	@Override
	public void store(Item item) throws IOException {
		items++;
		if (out != null) {
			out.println(item.toJSONString());
			out.flush();
		}
		if(items%1000==0) {
			open();	
		}
		
	}

	@Override
	public boolean delete(String id) throws IOException {
		// cannot delete rows from flat files
		return false;
	}

	@Override
	public void open() throws IOException {
		File storeFile = new File(storageDirectory, STORE_FILE+System.currentTimeMillis());
		out = new PrintWriter(new BufferedWriter(new FileWriter(storeFile, false)));
	}

	@Override
	public void close() {
		out.close();
	}

	@Override
	public void updateTimeslot() {

	}

	@Override
	public void update(Item update) throws IOException {
		items++;
		if (out != null) {
			out.println(update.toJSONString());
			out.flush();
		}
		if(items%1000==0) {
			open();	
		}
	}

}
