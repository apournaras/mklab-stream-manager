package eu.socialsensor.sfc.subscribers.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import twitter4j.FilterQuery;
import twitter4j.Paging;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.ResponseList;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;
import eu.socialsensor.framework.abstractions.socialmedia.twitter.TwitterItem;
import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.common.domain.Keyword;
import eu.socialsensor.framework.common.domain.Source;
import eu.socialsensor.framework.common.domain.Feed.FeedType;
import eu.socialsensor.framework.common.domain.Item.Operation;
import eu.socialsensor.framework.common.domain.feeds.KeywordsFeed;
import eu.socialsensor.framework.common.domain.feeds.LocationFeed;
import eu.socialsensor.framework.common.domain.feeds.SourceFeed;
import eu.socialsensor.sfc.streams.StreamConfiguration;
import eu.socialsensor.sfc.streams.StreamException;
import eu.socialsensor.sfc.subscribers.Subscriber;

/**
 * Class for retrieving real-time Twitter content by subscribing to Twitter. 
 * Twitter content can be based on keywords,twitter users or locations or be 
 * a random sampling of currently posted statuses. 
 * The retrieval process takes place through Twitter API (twitter4j)
 * @author ailiakop
 *
 */
public class TwitterSubscriber extends Subscriber {
	
	private Logger  logger = Logger.getLogger(TwitterSubscriber.class);
	
	public static long FILTER_EDIT_WAIT_TIME = 1000;
	
	private long lastFilterInitTime = System.currentTimeMillis();
	
	public enum AccessLevel {
		
		PUBLIC(400, 5000, 25),
		EXTENDED(400, 75000, 25);
		
		int filterMaxKeywords;
		int filterMaxFollows;
		int filterMaxLocations;
		
		private AccessLevel(int filterMaxKeywords,
						   int filterMaxFollows,
						   int filterMaxLocations) {
			this.filterMaxKeywords = filterMaxKeywords;
			this.filterMaxFollows = filterMaxFollows;
			this.filterMaxLocations = filterMaxLocations;
		}

		public int getFilterMaxKeywords() {
			return filterMaxKeywords;
		}

		public int getFilterMaxFollows() {
			return filterMaxFollows;
		}

		public int getFilterMaxLocations() {
			return filterMaxLocations;
		}

	}
	
	private AccessLevel accessLevel = AccessLevel.EXTENDED;
	private StatusListener listener = null;
	
	private twitter4j.TwitterStream twitterStream  = null;
	private Twitter twitterApi;
	
	public TwitterSubscriber() {
	
	}
	
	@Override
	public synchronized void subscribe(List<Feed> feeds) throws StreamException {
		
		if (twitterStream == null) {
			logger.error("Stream is closed");
			throw new StreamException("Stream is closed", null);
		} 
		else {
			List<String> keys = new ArrayList<String>();
			Set<String> users = new HashSet<String>();
			Set<Long> userids = new HashSet<Long>();
			List<double[]> locs = new ArrayList<double[]>();
			
			for(Feed feed : feeds) {
				if(feed.getFeedtype().equals(FeedType.KEYWORDS)) {
					if(((KeywordsFeed) feed).getKeyword() != null) {
						keys.add(((KeywordsFeed) feed).getKeyword().getName());
					}
					else {
						for(Keyword keyword : ((KeywordsFeed) feed).getKeywords()) {
							keys.add(keyword.getName());
						}
					}
						
				}
				else if(feed.getFeedtype().equals(FeedType.SOURCE)) {
					Source source = ((SourceFeed) feed).getSource();		
					if(source.getId() == null) {
						try {
							users.add(source.getName());
						}
						catch(Exception e) {
							logger.error(e.getMessage());
							continue;
						}
					}
					else {
						userids.add(Long.parseLong(source.getId()));
					}
				}
				else if(feed.getFeedtype().equals(FeedType.LOCATION)) {
					double[] location = new double[2];
					
					location[0] = ((LocationFeed) feed).getLocation().getLatitude();
					location[1] = ((LocationFeed) feed).getLocation().getLongitude();
					locs.add(location);
				}
			}
			
			Set<Long> temp = getUserIds(users);
			userids.addAll(temp);
			
			String[] keywords = new String[keys.size()];
			long[] follows = new long[Math.min(userids.size(), accessLevel.filterMaxFollows)];
			double[][] locations = new double[locs.size()][2];
			
			for(int i=0; i<keys.size(); i++) {
				keywords[i] = keys.get(i);
				if(i >= accessLevel.filterMaxKeywords) {
					break;
				}
			}
			
			int index = 0;
			for(Long userId : userids) {
				follows[index++] = userId;
				if(index >= accessLevel.filterMaxFollows) {
					break;
				}
			}
			
			for(int i=0; i<locs.size(); i++) {
				locations[i] = locs.get(i);
				if(i >= accessLevel.filterMaxLocations) {
					break;
				}
			}
			
			if (!ensureFilterLimits(keywords, follows, locations)) {
				logger.error("Filter exceeds Twitter's public access level limits");
				throw new StreamException("Filter exceeds Twitter's public access level limits");
			}

			FilterQuery filterQuery = getFilterQuery(keywords, follows, locations);
			if (filterQuery != null) {
				
				//getPastTweets(keywords, follows);
				
				if (System.currentTimeMillis() - lastFilterInitTime < FILTER_EDIT_WAIT_TIME) {
                     try {
                    	 logger.info("Wait for " + FILTER_EDIT_WAIT_TIME + " msecs to edit filter");
                    	 wait(FILTER_EDIT_WAIT_TIME);
					} catch (InterruptedException e) {
						logger.error(e.getMessage());
					}
				}
				lastFilterInitTime = System.currentTimeMillis();
				
				logger.info("Start tracking from twitter stream");
				twitterStream.shutdown();
				twitterStream.filter(filterQuery);
			
			}
			else {
				logger.info("Start sampling from twitter stream");
				twitterStream.sample();
			}
		}
		
	}
	
	
	public void getPastTweets(String[] keywords, long[] userids) {
		int maxReq  = 450;
		
		int totalRequests = 0;
		
		List<Status> tweets = new ArrayList<Status>();
		if(keywords != null && keywords.length>0) {
			Query query = new Query();
			query.setQuery(StringUtils.join(keywords, " OR "));
			query.setCount(100);
			do {
				System.out.println(query.toString());
				QueryResult resp;
				try {
					totalRequests++;
					resp = twitterApi.search(query);
					tweets.addAll(resp.getTweets());
					query = resp.nextQuery();
				} catch (TwitterException e) { 
					e.printStackTrace();
					break;
				}

			} while(query != null && totalRequests < maxReq);
		}

		if(userids != null) {
			int mapPagesPerUser = maxReq / userids.length;
			for(long userid : userids) {
				try {
					int page = 1, count = 100;
					Paging paging = new Paging(page, count);
					while(true) {
						totalRequests++;
						ResponseList<Status> timeline = twitterApi.getUserTimeline(userid, paging);
						tweets.addAll(timeline);

						System.out.println(paging.toString() + " => " + tweets.size());
						paging.setPage(++page);
						paging.setCount(100);
						
						if(timeline.size()<count || page>mapPagesPerUser 
								|| totalRequests>maxReq) {
							break;
						}

					}

				} catch (TwitterException e) {
					logger.error(e);
					break;
				}
			}
			for(Status status : tweets) {
				listener.onStatus(status);
			}
		}
	}
	
	private Set<Long> getUserIds(Collection<String> followsUsernames) {
		
		Set<Long> ids = new HashSet<Long>();
		
		List<String> usernames = new ArrayList<String>(followsUsernames.size());
		for(String username : followsUsernames) {
			usernames.add(username);
		}
		
		int size = usernames.size();
		int start = 0;
		int end = Math.min(start+100, size);
		
		while(start < size) {
			List<String> sublist = usernames.subList(start, end);
			String[] _usernames = sublist.toArray(new String[sublist.size()]);
			try {
				System.out.println("Request for " + _usernames.length + " users ");
				ResponseList<User> users = twitterApi.lookupUsers(_usernames);
				System.out.println(users.size() + " users ");
				for(User user : users) {
					long id = user.getId();
					ids.add(id);
				}
			} catch (TwitterException e) {
				logger.error("Error while getting user ids from twitter...");
				logger.error("Exception in getUserIds: ", e);
				break;
			}
			
			start = end + 1;
			end = Math.min(start+100, size);
		}
		
		return ids;
	}

	@Override
	public void stop() {
		if (listener != null) {
			if(twitterStream != null) {
				logger.info("Shutdown TwiterStream.");
				twitterStream.shutdown();
			}
			listener = null;
			twitterStream  = null;
		}
	}
	
	private StatusListener getListener() { 
		return new StatusListener() {
			long items = 0, deletion = 0;
			
			@Override
			public void onStatus(Status status) {
				synchronized(this) {
					if(status != null) {
						try {
							// Update original tweet in case of retweets
							Status retweetedStatus = status.getRetweetedStatus();
							if(retweetedStatus != null) {
								store(new TwitterItem(retweetedStatus));
							}
						
							// store
							if((++items)%5000==0) {
								logger.info(items + " incoming items from twitter. " + deletion + " deletions.");
							}
							store(new TwitterItem(status));
						}
						catch(Exception e) {
							logger.error("Exception onStatus: ", e);
						}
					}
				}
			}
			
			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
					try {
						deletion++;
						String id = Long.toString(statusDeletionNotice.getStatusId());
						TwitterItem update = new TwitterItem(id, Operation.DELETED);
						delete(update);
					}
					catch(Exception e) {
						logger.error("Exception onDeletionNotice: ", e);
					}
				
			}
			
			@Override
			public void onTrackLimitationNotice(int numOfLimitedStatuses) {
				synchronized(this) {
					logger.error("Rate limit: " + numOfLimitedStatuses);
				}
			}
			
			@Override
			public void onException(Exception ex) {
				synchronized(this) {
					ex.printStackTrace();
					logger.error("Internal stream error occured: " + ex.getMessage());
				}
			}
			@Override
			public void onScrubGeo(long userid, long id) {
				logger.info("Remove appropriate geolocation information for user " + userid + " up to tweet with id " + id);
			}

			@Override
			public void onStallWarning(StallWarning warn) {	
				if(warn != null) {
					logger.error("Stall Warning " + warn.getMessage() + "(" + warn.getPercentFull() + ")");
				}
			}
		};
	}

	
	private boolean ensureFilterLimits(String[] keywords, long[] follows, double[][] locations) {
		if (keywords != null && keywords.length > accessLevel.getFilterMaxKeywords()) 
			return false;
		if (follows != null && follows.length > accessLevel.getFilterMaxFollows()) 
			return false;
		if (locations != null && (locations.length/2) > accessLevel.getFilterMaxLocations()) 
			return false;
		
		return true;
	}
	
	private FilterQuery getFilterQuery(String[] keywords, long[] follows, double[][] locations) {
		FilterQuery query = new FilterQuery();
		boolean empty = true;
		if (keywords != null && keywords.length > 0) {
			logger.info(follows.length + " keywords to track.");
			query = query.track(keywords);
			empty = false;
		}
		
		if (follows != null && follows.length > 0) {
			logger.info(follows.length + " users to follow.");
			query = query.follow(follows);
			empty = false;
		}
		
		if (locations != null && locations.length > 0) {
			logger.info(locations.length + " locations to track.");
			query = query.locations(locations);
			empty = false;
		}
		
		if (empty) {
			return null;
		}
		else {
			return query;
		}
	}

	@Override
	public void open(StreamConfiguration config) throws StreamException {

		if (twitterStream != null) {
			logger.error("#Twitter : Stream is already opened");
			try {
				throw new StreamException("Stream is already opened", null);
			} catch (StreamException e) {
				e.printStackTrace();
			}
		}
		
		String oAuthConsumerKey 		= 	config.getParameter(KEY);
		String oAuthConsumerSecret 		= 	config.getParameter(SECRET);
		String oAuthAccessToken 		= 	config.getParameter(ACCESS_TOKEN);
		String oAuthAccessTokenSecret 	= 	config.getParameter(ACCESS_TOKEN_SECRET);
		
		String accessLevel = config.getParameter("AccessLevel");
		if(accessLevel != null && accessLevel.equals("public")) {
			this.accessLevel = AccessLevel.PUBLIC;
		}
		
		if (oAuthConsumerKey == null || oAuthConsumerSecret == null ||
				oAuthAccessToken == null || oAuthAccessTokenSecret == null) {
			logger.error("#Twitter : Stream requires authentication");
			throw new StreamException("Stream requires authentication");
		}
		
		logger.info("Twitter Credentials: \n" + 
				"\t\t\toAuthConsumerKey:  " + oAuthConsumerKey  + "\n" +
				"\t\t\toAuthConsumerSecret:  " + oAuthConsumerSecret  + "\n" +
				"\t\t\toAuthAccessToken:  " + oAuthAccessToken + "\n" +
				"\t\t\toAuthAccessTokenSecret:  " + oAuthAccessTokenSecret);
		
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setJSONStoreEnabled(false)
			.setOAuthConsumerKey(oAuthConsumerKey)
			.setOAuthConsumerSecret(oAuthConsumerSecret)
			.setOAuthAccessToken(oAuthAccessToken)
			.setOAuthAccessTokenSecret(oAuthAccessTokenSecret);
		Configuration conf = cb.build();
		
		listener = getListener();
		twitterStream = new TwitterStreamFactory(conf).getInstance();	
		twitterStream.addListener(listener);	
		
		twitterApi = new TwitterFactory(conf).getInstance();
	}

	@Override
	public String getName() {
		return "Twitter";
	}

}
