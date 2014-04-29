package eu.socialsensor.sfc.streams.store;

import java.io.IOException;
import java.util.Collection;
import java.util.Queue;

import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.Item.Operation;
import eu.socialsensor.sfc.streams.filters.ItemFilter;

/**
 * Class for storing items to databases
 * 
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 * @author ailiakop
 * @email  ailiakop@iti.gr
 *
 */
public class Consumer extends Thread {
	private boolean isAlive = true;
	private StreamUpdateStorage store = null;
	private Queue<Item> queue;
	
	private Collection<ItemFilter> filters;
	
	public Consumer(Queue<Item> queue, StreamUpdateStorage store, Collection<ItemFilter> filters){
		this.store = store;
		this.queue = queue;
		this.filters = filters;
	}
	
	/**
	 * Stores an item if the latter is found waiting in the queue
	 */
	public void run() {			
		Item item = null;
		while (isAlive) {
			try {
				item = poll();
				if (item == null) {
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) { }
					continue;
				} else {
					dump(item);
				}

			} catch(IOException e) {
				e.printStackTrace();
				
			}
		}
		
		//empty queue
		while ((item = poll()) != null) {
			try {
				dump(item);
			} catch (IOException e) {
				e.printStackTrace();
				
			}
		}
	}
	
	/**
	 * Stores an item to all available databases
	 * @param item
	 * @throws IOException
	 */
	private void dump(Item item) throws IOException {
		//dump update to store
		if (store != null) {
			for(ItemFilter filter : filters) {
				if(!filter.accept(item))
					return;
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
	 * Stops the consumer thread
	 * and closes all storages in case they have
	 * not yet been closed
	 */
	public synchronized void die() {
		isAlive = false;
		
		if(store != null){
			store.close();
		}
	}
}
