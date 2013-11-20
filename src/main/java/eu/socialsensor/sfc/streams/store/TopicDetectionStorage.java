package eu.socialsensor.sfc.streams.store;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import eu.socialsensor.framework.client.search.solr.SolrTopicDetectionItem;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.sfc.streams.StorageConfiguration;

public class TopicDetectionStorage implements StreamUpdateStorage {

	private Logger  logger = Logger.getLogger(SolrStorage.class);
	
	private static final String HOSTNAME = "solr.hostname";
	private static final String SERVICE = "solr.service";
	
	private static final String TOPICDETECTIONITEMS_COLLECTION = "solr.topicDetectionItems.collection";
	
	private String hostname, service;
	private String topicDetectionItemsCollection = null;
	
	private HttpSolrServer topicDetectionItemsServer = null;

	
	public TopicDetectionStorage(StorageConfiguration config) throws IOException {
		this.hostname = config.getParameter(TopicDetectionStorage.HOSTNAME);
		this.service = config.getParameter(TopicDetectionStorage.SERVICE);
		this.topicDetectionItemsCollection = config.getParameter(TopicDetectionStorage.TOPICDETECTIONITEMS_COLLECTION);
	}

	@Override
	public void open() throws IOException {
		if(topicDetectionItemsCollection != null) {
			topicDetectionItemsServer = new HttpSolrServer(hostname+"/"+service+"/"+topicDetectionItemsCollection);
		}
	}

	@Override
	public void store(Item item) throws IOException {
		SolrTopicDetectionItem solrTopicDetectionItem = new SolrTopicDetectionItem(item);
		try {
			if(topicDetectionItemsServer != null) {
				topicDetectionItemsServer.addBean(solrTopicDetectionItem);	
			}
		} catch(Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean delete(String itemId) throws IOException {
		try {
			if(topicDetectionItemsServer != null) {
				topicDetectionItemsServer.deleteById(itemId);
			}
		} catch (SolrServerException e) {
			return false;
		}
		return true;
	}

	@Override
	public void updateTimeslot() {
		try {
			topicDetectionItemsServer.optimize();
			topicDetectionItemsServer.commit();
		} catch (SolrServerException e) {
			
		} catch (IOException e) {
			logger.error(e);
		}
	}

	@Override
	public void close() {
		try {
			topicDetectionItemsServer.optimize();
			topicDetectionItemsServer.commit();
		} catch (SolrServerException e) {
			
		} catch (IOException e) {
			logger.error(e);
		}
	}

	public static void main(String[] args) {


	}

	@Override
	public void update(Item update) throws IOException {
		SolrTopicDetectionItem solrTopicDetectionItem = new SolrTopicDetectionItem(update);
		try {
			if(topicDetectionItemsServer != null) {
				topicDetectionItemsServer.addBean(solrTopicDetectionItem);	
			}
		} catch(Exception e) {
			throw new IOException(e);
		}
	}
}
