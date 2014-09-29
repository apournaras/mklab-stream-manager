package eu.socialsensor.sfc.input;

import java.util.List;
import java.util.Map;

import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.common.domain.Feed.FeedType;

/**
 * @brief The interface for the creation of input feeds
 * from various sources
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public interface InputReader {
	/**
	 * Maps the data from the source to feedtypes according to their type : (Keyword,Source, Location)
	 * @return the map of each object to its feedtype
	 */
	public Map<FeedType,Object> getData();
	
	/**
	 * Creates the mapping of the input feeds to each stream 
	 * @return the map of the created feeds to each stream
	 */
	public Map<String,List<Feed>> createFeedsPerStream();
	
	/**
	 * Creates the feeds for all streams together
	 * @return A list of feeds 
	 */
	public List<Feed> createFeeds();
	
}
