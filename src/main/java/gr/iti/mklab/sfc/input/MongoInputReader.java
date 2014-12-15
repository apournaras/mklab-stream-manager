package gr.iti.mklab.sfc.input;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import gr.iti.mklab.framework.client.dao.AccountDAO;
import gr.iti.mklab.framework.client.dao.RssSourceDAO;
import gr.iti.mklab.framework.common.domain.feeds.AccountFeed;
import gr.iti.mklab.framework.common.domain.feeds.Feed;
import gr.iti.mklab.framework.common.domain.feeds.KeywordsFeed;
import gr.iti.mklab.framework.common.domain.feeds.LocationFeed;
import gr.iti.mklab.framework.common.domain.feeds.URLFeed;
import gr.iti.mklab.framework.common.domain.feeds.Feed.FeedType;
import gr.iti.mklab.framework.common.domain.Account;
import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.common.domain.Location;
import gr.iti.mklab.framework.common.domain.Source;

/**
 * @brief The class responsible for the creation of input feeds from
 * mongo db storage
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class MongoInputReader implements InputReader {
	
	protected static final String SINCE = "since";
	
	protected static final String HOST = "host";
	protected static final String DB = "database";
	protected static final String SOURCES_COLLECTION = "sources.collection";
	protected static final String RSS_SOURCES_COLLECTION = "rss_sources.collection";
	protected static final String EXPERTS_COLLECTION = "experts.collection";
	protected static final String KEYWORDS_COLLECTION = "keywords.collection";
	
	private Configuration storage_config;
	
	private Set<String> streams = new HashSet<String>();
	
	private String host = null;
	private String db = null;
	private String newsHoundsCollection = null;
	private String expertsCollection = null;
	
	private String streamType = null;
	
	private Date sinceDate = null;
	
	private Map<String, List<Feed>> feedsPerStream = new HashMap<String, List<Feed>>();

	private String rssSourcesCollection;
	
	public MongoInputReader(Configuration config) {
		
		this.storage_config = config;
		
		config.getParameter("since");
		
		streams.add("Twitter");
		streams.add("Facebook");
		streams.add("RSS");
		streams.add("Tumblr");
		streams.add("Instagram");
		streams.add("GooglePlus");
		streams.add("Youtube");
		streams.add("Flickr");
		
		this.host = storage_config.getParameter(MongoInputReader.HOST);
		this.db = storage_config.getParameter(MongoInputReader.DB);
		this.newsHoundsCollection = storage_config.getParameter(MongoInputReader.SOURCES_COLLECTION, "Sources");
		this.rssSourcesCollection = storage_config.getParameter(MongoInputReader.RSS_SOURCES_COLLECTION, "RssSources");
		this.expertsCollection = storage_config.getParameter(MongoInputReader.EXPERTS_COLLECTION,"Experts");
		
	}
	
	@Override
	public Map<String, List<Feed>> createFeedsPerStream() {
	
		for(String stream : streams) {
		
			List<Feed> feeds = new ArrayList<Feed>();
			
			if(stream.equals("Twitter")) {
				this.streamType = Source.Twitter.name();
			}
			else if(stream.equals("Facebook")) {
				this.streamType = Source.Facebook.name();
			}
			else if(stream.equals("Flickr")) {
				this.streamType = Source.Flickr.name();
			}
			else if(stream.equals("GooglePlus")) {
				this.streamType = Source.GooglePlus.name();
			}
			else if(stream.equals("Instagram")) {
				this.streamType = Source.Instagram.name();
			}
			else if(stream.equals("Tumblr")) {
				this.streamType = Source.Tumblr.name();
			}
			else if(stream.equals("Youtube")) {
				this.streamType = Source.Youtube.name();
			}
			else if(stream.equals("RSS")) {
				this.streamType = Source.RSS.name();
			}
	
			Map<FeedType, Object> inputData = getData();

			for(FeedType feedType : inputData.keySet()) {

				switch(feedType) {
				
					case ACCOUNT :
						@SuppressWarnings("unchecked")
						List<Account> sources = (List<Account>) inputData.get(feedType);
						for(Account source : sources) {
							String feedID = source.getSource() + "#" + source.getName(); //UUID.randomUUID().toString();
							AccountFeed sourceFeed = new AccountFeed(source, sinceDate, feedID);			
							feeds.add(sourceFeed);
						}
						break;
					
					case URL :
						@SuppressWarnings("unchecked")
						List<String> rssSources = (List<String>) inputData.get(feedType);
						for(String url : rssSources) {
							String feedID = url;//UUID.randomUUID().toString();
							URLFeed sourceFeed = new URLFeed(url, sinceDate, feedID);
							feeds.add(sourceFeed);
						}
						break;
				
					case KEYWORDS : 
						@SuppressWarnings("unchecked")
						List<String> keywords = (List<String>) inputData.get(feedType);
						for(String keyword : keywords) {
							String feedID = UUID.randomUUID().toString();
							KeywordsFeed keywordsFeed = new KeywordsFeed(keyword, sinceDate, feedID);
							feeds.add(keywordsFeed);
						}
						break;
				
					case LOCATION :
						@SuppressWarnings("unchecked")
						List<Location> locations = (List<Location>) inputData.get(feedType);
						for(Location location : locations) {
							String feedID = UUID.randomUUID().toString();
							LocationFeed locationFeed = new LocationFeed(location, sinceDate, feedID);
							feeds.add(locationFeed);
						}
						break;
					
					default:
						break;
				}
			}
			feedsPerStream.put(stream, feeds);
		}
		
		return feedsPerStream;
	}
	
	@Override
	public List<Feed> createFeeds(){
		return null;
	}
	
	@Override
	public Map<FeedType, Object> getData() {
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
		String since = storage_config.getParameter(SINCE);
		if(since != null) {
			try {
				sinceDate = (Date) formatter.parse(since);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		
		Map<FeedType, Object> inputDataPerType = new HashMap<FeedType, Object>();
		
		if(host == null || db == null || newsHoundsCollection == null || expertsCollection == null){
			System.out.println("News hounds collection needs to be configured correctly");
			return null;
		}
		
		//sources
		List<Account> sources = new ArrayList<Account>();
		List<String> rssSources = new ArrayList<String>();
		
		if(streamType.equals("RSS")) {
			//rssSources.addAll(rssSourceDao.getRssSources());
		}
		else {
			//List<Account> streamSources = sourceDao.findTopAccounts(75000, Source.valueOf(streamType));
			//sources.addAll(streamSources);
		}
		
		// extract keywords
		List<String> keywords = new ArrayList<String>();
		
		if(!keywords.isEmpty()) {
			inputDataPerType.put(FeedType.KEYWORDS, keywords);
		}
		
		if(!sources.isEmpty()) {
			inputDataPerType.put(FeedType.ACCOUNT, sources);
		}
		
		if(!rssSources.isEmpty()) {
			inputDataPerType.put(FeedType.URL, rssSources);
		}
		
		return inputDataPerType;
	}
	
}