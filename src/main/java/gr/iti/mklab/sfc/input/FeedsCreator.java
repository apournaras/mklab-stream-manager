package gr.iti.mklab.sfc.input;

import java.util.List;
import java.util.Map;
import java.util.Set;

import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.common.domain.dysco.Dysco;
import gr.iti.mklab.framework.common.domain.feeds.Feed;


/**
 * @brief  The class is responsible for the creation of input feeds
 * that can result either from a configuration file input, 
 * a storage input(currently only mongo db is supported), a DySco or a txt file. 
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class FeedsCreator {
	
	private InputReader reader = null;
	private InputConfiguration config = null;
	private Dysco dysco = null;
	
	public <T> FeedsCreator(DataInputType dataInputType, T inputData) {
		
		switch(dataInputType) {
			case CONFIG_FILE:
				this.config = (InputConfiguration) inputData;
				if(this.config == null) {
					System.out.println("Input Configuration is not set");
					return;
				}
				
				Set<String> streamInputs = config.getStreamInputIds();
				if(!streamInputs.isEmpty()) {
					reader = new ConfigInputReader(config);
				}
				else {
					System.err.println("Streams need to be configured");
					return;
				}
				break;
			case TXT_FILE:
				
				this.config = (InputConfiguration) inputData;
				if(this.config == null) {
					System.out.println("Input Configuration is not set");
					return;
				}
				
				Set<String> newsCollectors = config.getStreamInputIds();
				if(!newsCollectors.isEmpty()) {
					reader = new FileInputReader(config);
				}
				else {
					System.err.println("News Collectors need to be configured");
					return;
				}
				break;
			case MONGO_STORAGE:
				this.config = (InputConfiguration) inputData;
				
				if(this.config == null) {
					System.out.println("Input Configuration is not set");
					return;
				}
				
				Set<String> storageInputs = config.getStorageInputIds();
				if(!storageInputs.isEmpty()) {
					for(String storageId : storageInputs) {
						if(storageId.equals("Mongodb")) {
							Configuration m_conf = config.getStorageInputConfig("Mongodb");
							if(m_conf != null) {
								reader = new MongoInputReader(m_conf);
							}
						}
					}
				}
				else {
					System.err.println("Storage needs to be configured");
					return;
				}
				
				break;
				
			case DYSCO:
				this.dysco = (Dysco) inputData;
				reader = new DyscoInputReader(this.dysco);
				break;
		}
	}
	
	/**
	 * Returns the input feeds created mapped to each stream 
	 * @return A map of the input feeds to each stream
	 */
	public Map<String,List<Feed>> getQueryPerStream(){
		if(reader == null)
			return null;
		
		return reader.createFeedsPerStream();
	}
	
	/**
	 * Returns the input feeds created for all streams together
	 * @return the input feeds
	 */
	public List<Feed> getQuery(){
		if(reader == null)
			return null;
		
		return reader.createFeeds();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
	}

}
