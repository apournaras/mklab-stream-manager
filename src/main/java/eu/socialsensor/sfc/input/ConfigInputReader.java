package eu.socialsensor.sfc.input;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import eu.socialsensor.framework.Configuration;
import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.common.domain.Keyword;
import eu.socialsensor.framework.common.domain.Location;
import eu.socialsensor.framework.common.domain.SocialNetworkSource;
import eu.socialsensor.framework.common.domain.Source;
import eu.socialsensor.framework.common.domain.Feed.FeedType;
import eu.socialsensor.framework.common.domain.feeds.KeywordsFeed;
import eu.socialsensor.framework.common.domain.feeds.LocationFeed;
import eu.socialsensor.framework.common.domain.feeds.SourceFeed;

/**
 * @brief The class that is responsible for the creation of input feeds
 * from a configuration file 
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class ConfigInputReader implements InputReader{
	protected static final String SINCE = "since";
	protected static final String KEYWORDS = "keywords";
	protected static final String FOLLOWS = "follows";
	protected static final String LOCATION = "locations";
	protected static final String FEED_LIST = "feedsSeedlist";
	
	private InputConfiguration config;
	
	private Set<String> streams = null;
	
	private SocialNetworkSource streamType = null;
	
	private Configuration stream_config = null;
	
	private Date sinceDate = null;
	
	private Map<String,List<Feed>> feeds = new HashMap<String,List<Feed>>();
	
	public ConfigInputReader(InputConfiguration config){
		this.config = config;
		streams = config.getStreamInputIds();

	}
	
	@Override
	public Map<String,List<Feed>> createFeedsPerStream(){
		
		for(String stream : streams){
			List<Feed> feedsPerStream = new ArrayList<Feed>();
			
			if(stream.equals("Twitter"))
				this.streamType = SocialNetworkSource.Twitter;
			else if(stream.equals("Facebook"))
				this.streamType = SocialNetworkSource.Facebook;
			else if(stream.equals("Flickr"))
				this.streamType = SocialNetworkSource.Flickr;
			else if(stream.equals("GooglePlus"))
				this.streamType = SocialNetworkSource.GooglePlus;
			else if(stream.equals("Instagram"))
				this.streamType = SocialNetworkSource.Instagram;
			else if(stream.equals("Tumblr"))
				this.streamType = SocialNetworkSource.Tumblr;
			else if(stream.equals("Topsy"))
				this.streamType = SocialNetworkSource.Topsy;
			else if(stream.equals("Youtube"))
				this.streamType = SocialNetworkSource.Youtube;
			
			Map<FeedType,Object> inputData = getData();
			
			for(FeedType feedType : inputData.keySet()){
				switch(feedType){
				case SOURCE :
					@SuppressWarnings("unchecked")
					List<Source> sources = (List<Source>) inputData.get(feedType);
					for(Source source : sources){
						source.setNetwork(streamType.toString());
						String feedID = UUID.randomUUID().toString();
						SourceFeed sourceFeed = new SourceFeed(source,sinceDate,feedID);
						feedsPerStream.add(sourceFeed);
					}
					break;
				case KEYWORDS : 
					@SuppressWarnings("unchecked")
					List<Keyword> keywords = (List<Keyword>) inputData.get(feedType);
					for(Keyword keyword : keywords){
						String feedID = UUID.randomUUID().toString();
						KeywordsFeed keywordsFeed = new KeywordsFeed(keyword,sinceDate,feedID);
						feedsPerStream.add(keywordsFeed);
					}
					break;
				case LOCATION :
					@SuppressWarnings("unchecked")
					List<Location> locations = (List<Location>) inputData.get(feedType);
					for(Location location : locations){
						String feedID = UUID.randomUUID().toString();
						LocationFeed locationFeed = new LocationFeed(location,sinceDate,feedID);
						feedsPerStream.add(locationFeed);
					}
					break;
				default:
					break;
				}
			}
			feeds.put(stream, feedsPerStream);
		}
		
		return feeds;
	}
	
	@Override
	public List<Feed> createFeeds(){
		return null;
	}
	
	
	@Override
	public Map<FeedType,Object> getData(){
		Map<FeedType,Object> inputDataPerType = new HashMap<FeedType,Object>();
		
		this.stream_config = config.getStreamInputConfig(streamType.toString());
	
		String value;
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
		String since = stream_config.getParameter(SINCE);
		if(since != null){
			try {
				sinceDate = (Date) formatter.parse(since);
				
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		
		//sources
		List<Source> sources = new ArrayList<Source>();
		
		//users by feedList
		value = stream_config.getParameter(FEED_LIST);
		if (value != null && !value.equals("")) {
			List<String> users = new ArrayList<String>();
			BufferedReader br = null;
			try {
				br = new BufferedReader(new FileReader(value));
				
		        String line = br.readLine();
		        users.add(line);
		        System.out.println("user : "+line);
		        while (line != null) {
		         
		            line = br.readLine();
		            if(line != null){
		            	users.add(line);
		            }
		        }
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally {
		        try {
					br.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    }
			
			for(String user : users) {
				sources.add(new Source(user, 0.0f)); 	
			}
			
		}
		
		//users by config file
		value = stream_config.getParameter(FOLLOWS);
		
		if (value != null && !value.equals("")) {
			
			if(value.contains(",")){
				
				String[] users = value.split(",");
			
				for(String user : users) {
					sources.add(new Source(user, 0.0f)); 	
				}
			}
			else{
				String user = value;
				sources.add(new Source(user, 0.0f)); 	
			}
		}
	
		if(!sources.isEmpty())
			inputDataPerType.put(FeedType.SOURCE, sources);
		
		//keywords
		List<Keyword> keywords = new ArrayList<Keyword>();
		
		value = stream_config.getParameter(KEYWORDS);
		if (value != null && !value.equals("")) {
			String[] tokens = value.split(",");
			
			for(String token : tokens) {
				keywords.add(new Keyword(token.toLowerCase(), 0.0f));
			}
		}
		
		if(!keywords.isEmpty())
			inputDataPerType.put(FeedType.KEYWORDS, keywords);
		
		//locations
		List<Location> locations = new ArrayList<Location>();
		
		value = stream_config.getParameter(LOCATION);
		if (value != null && !value.equals("")) {
			
			String[] parts = value.split(";");
			for (String part : parts) {
				part = part.trim();
				String[] subparts = part.split(",");
				
				// invert google coordinates
				double latitude = Double.parseDouble(subparts[0]);
				double longitude = Double.parseDouble(subparts[1]);
				
				locations.add(new Location(latitude, longitude));
			
			}
		}
		
		if(!locations.isEmpty()){
			inputDataPerType.put(FeedType.LOCATION, locations);
		}
			
		
		return inputDataPerType;
	}
	
}
