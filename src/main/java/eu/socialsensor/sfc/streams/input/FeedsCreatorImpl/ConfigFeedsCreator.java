package eu.socialsensor.sfc.streams.input.FeedsCreatorImpl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.common.domain.Keyword;
import eu.socialsensor.framework.common.domain.Location;
import eu.socialsensor.framework.common.domain.Source;
import eu.socialsensor.framework.common.domain.feeds.KeywordsFeed;
import eu.socialsensor.framework.common.domain.feeds.LocationFeed;
import eu.socialsensor.framework.common.domain.feeds.SourceFeed;
import eu.socialsensor.framework.streams.StreamConfiguration;
import eu.socialsensor.sfc.streams.input.FeedsCreator;

/**
 * Class for feed creation from configuration file
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */

public class ConfigFeedsCreator implements FeedsCreator{
	private Logger logger = Logger.getLogger(ConfigFeedsCreator.class);
	
	protected static final String SINCE = "since";
	protected static final String KEYWORDS = "keywords";
	protected static final String FOLLOWS = "follows";
	protected static final String LOCATION = "locations";
	
	private StreamConfiguration configFile;
	
	private List<Keyword> extractedKeywords = new ArrayList<Keyword>();
	private List<Source> extractedSources = new ArrayList<Source>();
	private List<Location> extractedLocations = new ArrayList<Location>();
	
	private Date sinceDate = null;
	
	public ConfigFeedsCreator(StreamConfiguration configFile){
		this.configFile = configFile;
	}
	
	@Override
	public List<String> extractKeywords(){
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
		String since = configFile.getParameter(SINCE);
		
		if(since != null){
			try {
				sinceDate = (Date) formatter.parse(since);
				
			} catch (ParseException e) {
				logger.error("ParseException : "+e);
			}
		}
		
	
		String value;
		
		value = configFile.getParameter(KEYWORDS);
		if (value != null && !value.equals("")) {
			String[] tokens = value.split(",");
			for(String token : tokens) {
				Keyword keyword = new Keyword(token.toLowerCase(), 0.0f);
				extractedKeywords.add(keyword);
			}
		}
		
		value = configFile.getParameter(FOLLOWS);
		if (value != null && !value.equals("")) {
			String[] users = value.split(",");
			for(String user : users) {
				Source source = new Source(user.toLowerCase(), 0.0f);
				extractedSources.add(source);
			}
		}
		
		value = configFile.getParameter(LOCATION);
		if (value != null && !value.equals("")) {
			
			String[] parts = value.split(";");
			for (String part : parts) {
				part = part.trim();
				String[] subparts = part.split(",");
				
				// invert google coordinates
				double latitude = Double.parseDouble(subparts[0]);
				double longitude = Double.parseDouble(subparts[1]);
				
				Location location = new Location(latitude, longitude);
				extractedLocations.add(location);
			}
			
		}
		
		return null;
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
