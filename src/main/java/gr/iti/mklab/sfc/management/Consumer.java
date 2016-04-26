package gr.iti.mklab.sfc.management;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.sfc.filters.ItemFilter;
import gr.iti.mklab.sfc.processors.Processor;
import gr.iti.mklab.sfc.storages.Storage;

/**
 * Class for storing items to databases
 * 
 * 
 * @author manosetro - manosetro@iti.gr
 *
 */
public class Consumer extends Thread {
	
	private Logger _logger = LogManager.getLogger(Consumer.class);
	
	private static int id = 0;
	
	private boolean isAlive = true;
	private List<Storage> storages = null;
	
	private BlockingQueue<Item> queue;
	
	private Collection<ItemFilter> filters;
	private Collection<Processor> processors;
	
	private long lastAccess = 0;
	private long itemsConsumed = 0L;
	private int lastAction = 0;
	private String[] actions = {"initialization", "taking from queue", "filtering", "running processors", "storing", " handling"};
	
	public Consumer(BlockingQueue<Item> queue, List<Storage> storages, Collection<ItemFilter> filters, Collection<Processor> processors) {
		this.storages = storages;
		this.queue = queue;
		this.filters = filters;
		this.processors = processors;
		
		this.setName("Consumer_" + (id++));
	}
	
	/**
	 * Stores an item if the latter is found waiting in the queue
	 */
	public void run() {			
		Item item = null;
		while (isAlive) {
			try {
				item = queue.take();
				if (item == null) {
					_logger.error("Item is null.");
				} 
				else {
					lastAction = 1;
					lastAccess = System.currentTimeMillis();
					itemsConsumed++;
					
					process(item);
				}
			} catch(IOException e) {
				_logger.error(e);
			} catch (InterruptedException e) {
				_logger.error(e);
			}
		}
		
		//empty queue before exit 
		while ((item = queue.poll()) != null) {
			try {
				process(item);
			} catch (IOException e) {
				_logger.error(e);
			}
		}
	}
	
	private void process(Item item) throws IOException {
		if (storages != null) {
			for(ItemFilter filter : filters) {
				boolean accept = true;
				synchronized(filter) {
					accept = filter.accept(item);
				}
				if(!accept) {
					return;
				}
			}
			lastAction = 2;
			
			for(Processor processor : processors) {
				synchronized(processor) {
					processor.process(item);	
				}
			}
			lastAction = 3;
			
			for(Storage storage : storages) {
				synchronized(storage) {
					storage.store(item);
				}
			}
			lastAction = 4;
		}
		else {
			_logger.error("Sorages list in null. Cannot process item.");
		}
		
	}
	
	/**
	 * Stops the consumer thread
	 */
	public synchronized void die() {
		isAlive = false;
		try {
			this.interrupt();
		}
		catch(Exception e) {
			_logger.error(e);
		}
	}
	
	private Date getLastAccess() {
		return new Date(lastAccess);
	}

	private long getConsumedItems() {
		return itemsConsumed;
	}
	
	public String status() {
		return getName() + " consumed " + getConsumedItems() + ". Last access [" + getLastAccess() 
				+ "]. Current state: " + getState() + " after " + actions[lastAction];
	}
}
