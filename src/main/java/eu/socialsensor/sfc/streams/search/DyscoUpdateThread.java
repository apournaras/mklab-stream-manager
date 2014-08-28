package eu.socialsensor.sfc.streams.search;

import java.util.Queue;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.client.search.solr.SolrDyscoHandler;
import eu.socialsensor.framework.common.domain.dysco.Dysco;

/**
	 * Class responsible for updating a DySco's solr queries after they have been computed from the
	 * query builder (query expansion module)
	 * @author ailiakop
	 *
	 */
	public class DyscoUpdateThread extends Thread {
		
		public final Logger logger = Logger.getLogger(DyscoUpdateThread.class);
		
		private SolrDyscoHandler solrdyscoHandler;
		private boolean isAlive = true;

		private Queue<Dysco> dyscosToUpdate;
		
		public DyscoUpdateThread(String solrServiceUrl, Queue<Dysco> dyscosToUpdate) {
			this.solrdyscoHandler = SolrDyscoHandler.getInstance(solrServiceUrl);
			this.dyscosToUpdate = dyscosToUpdate;
		}
		
		public void run() {
			Dysco dyscoToUpdate = null;
			
			while(isAlive) {
				dyscoToUpdate = poll();
				if(dyscoToUpdate == null) {
					try {
						synchronized(this) {
							this.wait(1000);
						}
					} catch (InterruptedException e) {
						logger.error(e);
					}
					continue;
				}
				else {
					try {
						Dysco previousDysco = solrdyscoHandler.findDyscoLight(dyscoToUpdate.getId());
						previousDysco.getSolrQueries().clear();
						previousDysco.setSolrQueries(dyscoToUpdate.getSolrQueries());
						solrdyscoHandler.insertDysco(previousDysco);
					
						logger.info("Dysco: " + dyscoToUpdate.getId() + " is updated");
					}
					catch(Exception e) {
						logger.error(e);
					}
				}
			}
		}
		
		/**
		 * Polls a DySco request from the queue to update
		 * @return
		 */
		private Dysco poll() {				
			return dyscosToUpdate.poll();
		}
		
		public void close() {
			isAlive = false;
			try {
				
			}
			catch(Exception e) {
				logger.error("Failed to interrupt itself", e);
			}
		}
	}