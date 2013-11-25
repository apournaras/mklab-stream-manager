package eu.socialsensor.sfc.streams.input.FeedsCreatorImpl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import eu.socialsensor.framework.client.dao.SourceDAO;
import eu.socialsensor.framework.client.dao.impl.SourceDAOImpl;
import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.common.domain.Keyword;
import eu.socialsensor.framework.common.domain.Location;
import eu.socialsensor.framework.common.domain.Source;
import eu.socialsensor.framework.common.domain.feeds.KeywordsFeed;
import eu.socialsensor.framework.common.domain.feeds.LocationFeed;
import eu.socialsensor.framework.common.domain.feeds.SourceFeed;
import eu.socialsensor.sfc.streams.StorageConfiguration;
import eu.socialsensor.sfc.streams.StreamsManagerConfiguration;
import eu.socialsensor.sfc.streams.input.FeedsCreator;

public class MongoFeedCreator implements FeedsCreator{
	
	protected static final String HOST = "mongodb.host";
	protected static final String DB = "mongodb.database";
	protected static final String SOURCES_COLLECTION = "mongodb.sources.collection";
	
	private StreamsManagerConfiguration configFile;
	private StorageConfiguration storageConfig;
	
	private String host = null;
	private String db = null;
	private String newsHoundsCollection = null;
	private Source.Type streamType = null;
	
	private Date sinceDate = null;
	
	private List<Keyword> extractedKeywords = new ArrayList<Keyword>();
	private List<Source> extractedSources = new ArrayList<Source>();
	private List<Location> extractedLocations = new ArrayList<Location>();
	
	public MongoFeedCreator(StreamsManagerConfiguration configFile){
		this.configFile = configFile;
		
		storageConfig = configFile.getStorageConfig("Mongodb");
	}
	
	public void setTypeOfStream(String streamType){
		
		if(streamType.equals("Twitter"))
			this.streamType = Source.Type.Twitter;
		else if(streamType.equals("Facebook"))
			this.streamType = Source.Type.Facebook;
		else if(streamType.equals("Flickr"))
			this.streamType = Source.Type.Flickr;
		else if(streamType.equals("GooglePlus"))
			this.streamType = Source.Type.GooglePlus;
		else if(streamType.equals("Instagram"))
			this.streamType = Source.Type.Instagram;
		else if(streamType.equals("Tumblr"))
			this.streamType = Source.Type.Tumblr;
		else if(streamType.equals("Youtube"))
			this.streamType = Source.Type.Youtube;
			
	}
	
	@Override
	public List<Source> extractFeedInfo(){
		this.host = storageConfig.getParameter(MongoFeedCreator.HOST);
		this.db = storageConfig.getParameter(MongoFeedCreator.DB);
		this.newsHoundsCollection = storageConfig.getParameter(MongoFeedCreator.SOURCES_COLLECTION, "Sources");
		
		extractedSources.clear();
		
		if(host == null || db == null || newsHoundsCollection == null){
			System.out.println("News hounds collection needs to be configured correctly");
			return null;
		}
		
		SourceDAO sourceDao = new SourceDAOImpl("social1.atc.gr", db, newsHoundsCollection);	
		
		List<Source> sources = sourceDao.findTopSources(5000,streamType);
		extractedSources.addAll(sources);
		
		return sources;
	}
	@Override
	public List<Feed> createFeeds(){
		List<Feed> feeds = new ArrayList<Feed>();
		
		for(Keyword keyword : extractedKeywords){
			String feedID = UUID.randomUUID().toString();
			KeywordsFeed keywordsFeed = new KeywordsFeed(keyword,sinceDate,feedID,null);
			feeds.add(keywordsFeed);
		}
		
		for(Source source : extractedSources){
			String feedID = UUID.randomUUID().toString();
			SourceFeed sourceFeed = new SourceFeed(source,sinceDate,feedID,null);
			feeds.add(sourceFeed);
		}
		
		for(Location location : extractedLocations){
			String feedID = UUID.randomUUID().toString();
			LocationFeed locationFeed = new LocationFeed(location,sinceDate,feedID);
			feeds.add(locationFeed);
		}
		
		return feeds;
	}

}
