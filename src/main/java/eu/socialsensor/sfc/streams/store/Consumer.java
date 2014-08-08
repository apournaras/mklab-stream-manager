package eu.socialsensor.sfc.streams.store;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.Item.Operation;
import eu.socialsensor.sfc.streams.filters.ItemFilter;
import eu.socialsensor.sfc.streams.processors.Processor;

/**
 * Class for storing items to databases
 * 
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 * 
 * @author ailiakop
 * @email  ailiakop@iti.gr
 *
 */
public class Consumer extends Thread {
	
	private Logger _logger = Logger.getLogger(Consumer.class);
	
	private boolean isAlive = true;
	private StreamUpdateStorage store = null;
	
	private BlockingQueue<Item> queue;
	
	private Collection<ItemFilter> filters;
	private Collection<Processor> processors;
	
	public Consumer(BlockingQueue<Item> queue, StreamUpdateStorage store, Collection<ItemFilter> filters, Collection<Processor> processors) {
		this.store = store;
		this.queue = queue;
		this.filters = filters;
		this.processors = processors;
	}
	
	/**
	 * Stores an item if the latter is found waiting in the queue
	 */
	public void run() {			
		Item item = null;
		while (isAlive) {
			try {
				item = take();
				if (item == null) {
					_logger.error("Item is null.");
				} else {
					process(item);
				}

			} catch(IOException e) {
				e.printStackTrace();
				_logger.error(e);
			}
		}
		
		//empty queue
		while ((item = poll()) != null) {
			try {
				process(item);
			} catch (IOException e) {
				e.printStackTrace();
				_logger.error(e);
			}
		}
	}
	
	/**
	 * Stores an item to all available databases
	 * @param item
	 * @throws IOException
	 */
	private void process(Item item) throws IOException {
		if (store != null) {
			for(ItemFilter filter : filters) {
				if(!filter.accept(item))
					return;
			}
			
			for(Processor processor : processors) {
				processor.process(item);	
			}
			
			if (item.getOperation() == Operation.NEW) {
				store.store(item);
			} 
			else if (item.getOperation() == Operation.UPDATE) {
				store.update(item);
			}
			else if (item.getOperation() == Operation.DELETED) {
				store.delete(item.getId());
			}
			else {
				System.out.println(item.getOperation() + ": Not supported operation");		
			}
		}
	}
	
	/**
	 * Polls an item from the queue
	 * @return
	 */
	private Item poll() {
		synchronized (queue) {					
			Item item = queue.poll();		
			return item;
		}
	}
	
	/**
	 * Polls an item from the queue. Waits if the queue is empty. 
	 * @return
	 */
	private Item take() {
		synchronized (queue) {					
			Item item = null;
			try {
				item = queue.take();
			} catch (InterruptedException e) {
				_logger.error(e);
			}	
			return item;
		}
	}
	
	/**
	 * Stops the consumer thread
	 */
	public synchronized void die() {
		isAlive = false;
	}
}
