package gr.iti.mklab.sfc.streams.monitors;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ItemsMonitor implements Runnable {

	public final Logger logger = LogManager.getLogger(ItemsMonitor.class);
			
	private BlockingQueue<Pair<Pair<String, String>, String>> itemsQueue;
	private Map<String, StreamFetchTask> streamsFetchTasks = new HashMap<String, StreamFetchTask>();
	
	private boolean stop = false;
	
	public ItemsMonitor(BlockingQueue<Pair<Pair<String, String>, String>> itemsQueue) {
		this.itemsQueue = itemsQueue;
	}

	public void addFetchTasks(Map<String, StreamFetchTask> streamsFetchTasks) {
		this.streamsFetchTasks.putAll(streamsFetchTasks);
	}
	
	@Override
	public void run() {
		while(!stop) {
			
			try {
				Pair<Pair<String, String>, String> pair = itemsQueue.take();
				
				String id = pair.getLeft().getLeft();
				String streamId = pair.getLeft().getRight();
				
				String action = pair.getRight();
				
				logger.info("Item " + streamId + "#" + id + " received for monitoring. Action: " + action);
				
				StreamFetchTask fetchTask = streamsFetchTasks.get(streamId);
				if(fetchTask == null) {
					logger.warn("Cannot add " + id + " to " + streamId + " fetch task.");
					continue;
				}
				
				switch (action) {
				
					case "items:new":
						fetchTask.addItem(id);
						break;
					
					case "items:delete":
						fetchTask.removeItem(id);
						break;	
						
				}
			} catch (InterruptedException e) {
				logger.error(e);
			}
			
		}
	}
	
	public void start() {
		Thread thread = new Thread(this);
		thread.start();
	}
	
	public void stop() {
		stop = true;
		Thread.currentThread().interrupt();
	}
}
