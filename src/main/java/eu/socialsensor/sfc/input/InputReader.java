package eu.socialsensor.sfc.input;

import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.common.domain.Feed.FeedType;
import eu.socialsensor.framework.common.domain.StreamUser.Category;

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
	
	/**
	 * Maps the users to follow to the newshounds lists they
	 * are included
	 * @return the map of the user to the newshounds lists
	 * he is included in
	 */
	public Map<String,Set<String>> getUsersToLists();
	
	/**
	 * Maps the users to follow to a category 
	 * @return the map of the user to the category he corresponds to
	 */
	public Map<String,Category> getUsersToCategories();
	
}
