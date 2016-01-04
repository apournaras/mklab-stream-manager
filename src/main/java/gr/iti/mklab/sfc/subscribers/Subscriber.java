package gr.iti.mklab.sfc.subscribers;

import java.util.Set;

import gr.iti.mklab.framework.common.domain.feeds.Feed;
import gr.iti.mklab.sfc.streams.Stream;
import gr.iti.mklab.sfc.streams.StreamException;

/**
 * The interface for retrieving content by subscribing to a social network channel.
 * Currently the only API that supports subscribing is Twitter API.
 * 
 * @author manosetro
 *
 */
public abstract class Subscriber extends Stream {

	public abstract void subscribe(Set<Feed> feeds) throws StreamException;

	public abstract void stop();

}
