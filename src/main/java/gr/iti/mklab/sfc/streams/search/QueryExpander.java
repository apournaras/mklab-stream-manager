package gr.iti.mklab.sfc.streams.search;

import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.dysco.Dysco;


public class QueryExpander extends Thread {

	public final Logger logger = Logger.getLogger(QueryExpander.class);
	
	private ExecutorService executor;
	private Queue<Dysco> dyscosToUpdate = new LinkedBlockingQueue<Dysco>();
	
	private boolean isAlive = true;
	private int queryNumberLimit = 5;
	
	protected ConcurrentHashMap<String, Future<Dysco>> tasks = new ConcurrentHashMap<String, Future<Dysco>>();
	
	public QueryExpander() {
		this.executor = Executors.newFixedThreadPool(15);
		//this.executor = Executors.newCachedThreadPool();
		
		this.setName("QueryExpander");
	}
	
	public QueryExpander(int queryNumberLimit) {
		this.queryNumberLimit = queryNumberLimit;
		executor = Executors.newFixedThreadPool(15);
		this.setName("QueryExpander");
	}
	
	public void addDysco(Dysco dysco, List<Item> items) {
		if(!tasks.containsKey(dysco.getId()) && !items.isEmpty()) {
			QueryExpansionTask expansionTask = new QueryExpansionTask(dysco, items);
			Future<Dysco> response = executor.submit(expansionTask);
			tasks.put(dysco.getId(), response);
		}
	}
	
	public void run() {
		while(isAlive) {
			Set<String> completed = new HashSet<String>();
			for(String dyscoId : tasks.keySet()) {
				Future<Dysco> response = tasks.get(dyscoId);
				try {
					if(response.isDone()) {
						Dysco dysco = response.get();
						if(dysco == null) {
							// Task has not finished yet. Continue to the next task.
							continue;
						}
						
						completed.add(dyscoId);
						
					}
					else if(response.isCancelled()) {
						completed.add(dyscoId);
					}
					
				} catch (Exception e) {
					logger.error("Error during processing of dysco: " + dyscoId);
					logger.error("Exception: " + e.getMessage());
					completed.add(dyscoId);
				}
			}
			
			// Remove completed tasks.
			for(String dyscoId : completed) {
				tasks.remove(dyscoId);
			}
			
			try {
				synchronized(this) {
					this.wait(1000);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void close() {
		isAlive = false;
		executor.shutdownNow();
		try {
			this.interrupt();
		}
		catch(Exception e) {
			logger.error("Failed to interrupt itself: " + e.getMessage());
		}
	}
	
	public void status() {
		logger.info("Executor is shutdown: " + executor.isShutdown());
		logger.info("Tasks: " + tasks.size());

		logger.info("dyscosToUpdate: " + dyscosToUpdate.size());
	}
	
	public class QueryExpansionTask implements  Callable<Dysco> {
		
		private Dysco dysco;
		private List<Item> items;

		public QueryExpansionTask(Dysco dysco, List<Item> items) {
			this.dysco = dysco;
			this.items = items;
		}
		
		@Override
		public Dysco call() throws Exception {
			try {
				long t = System.currentTimeMillis();
				//SolrQueryBuilder queryBuilder = new SolrQueryBuilder();
				//List<Query> queries = queryBuilder.getExpandedSolrQueries(items, dysco, queryNumberLimit);
				
				long t2 = System.currentTimeMillis();
				logger.info("Time for computing queries for Trending DySco: " + dysco.getId() + " is " + (t2-t)/1000 + " sec.");
				
				//dysco.setSolrQueries(queries);
			}
			catch(Exception e) {
				logger.error(e.getMessage());
			}
			return dysco;
		}
		
	}
	
	public Dysco getDyscoToUpdate() {
		return dyscosToUpdate.poll();
	}
	
}
