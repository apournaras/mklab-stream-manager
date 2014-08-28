package eu.socialsensor.sfc.subscribers;

import java.util.List;

import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.sfc.streams.Stream;
import eu.socialsensor.sfc.streams.StreamException;

/**
 * The interface for retrieving content by subscribing to a social network channel.
 * Currently the only API that supports subscribing is Twitter API.
 * @author ailiakop
 *
 */
public abstract class Subscriber extends Stream {

	/**
	 * Retrieves and stores relevant real-time content to a list of feeds by subscribing
	 * to a social network channel. 
	 * @param feed
	 * @throws StreamException
	 */
	public abstract void subscribe(List<Feed> feeds) throws StreamException;

	public abstract void stop();

}
