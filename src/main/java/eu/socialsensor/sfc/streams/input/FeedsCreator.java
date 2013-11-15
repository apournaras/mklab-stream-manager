package eu.socialsensor.sfc.streams.input;

import java.util.List;

import eu.socialsensor.framework.common.domain.Feed;

/**
 * @brief : Interface for the creation of feeds
 * @author ailiakop
 * @email ailiakop@iti.gr
 */

public interface FeedsCreator {
	
	/**
	 * Extracts the keywords that will be used for feeds' creation
	 * @param <T>
	 * @return List of Keywords
	 */
	public <T> List<Class<T>> extractFeedInfo();
	
	/**
	 * Creates the feeds that will be used as input for the wrappers
	 * @return List of feeds
	 */
	public List<Feed> createFeeds();
	
}
