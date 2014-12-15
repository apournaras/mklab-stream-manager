package gr.iti.mklab.sfc.input;

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

import gr.iti.mklab.framework.common.domain.config.Configuration;
import gr.iti.mklab.framework.common.domain.Location;
import gr.iti.mklab.framework.common.domain.Source;
import gr.iti.mklab.framework.common.domain.Account;
import gr.iti.mklab.framework.common.domain.feeds.AccountFeed;
import gr.iti.mklab.framework.common.domain.feeds.Feed;
import gr.iti.mklab.framework.common.domain.feeds.KeywordsFeed;
import gr.iti.mklab.framework.common.domain.feeds.LocationFeed;
import gr.iti.mklab.framework.common.domain.feeds.Feed.FeedType;

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
	
	private Source source = null;
	
	private Configuration stream_config = null;
	
	private Date sinceDate = null;
	
	private Map<String,List<Feed>> feeds = new HashMap<String,List<Feed>>();
	
	public ConfigInputReader(InputConfiguration config){
		this.config = config;
		streams = config.getStreamInputIds();

	}
	
	@Override
	public Map<String, List<Feed>> createFeedsPerStream(){
		
		for(String stream : streams){
			List<Feed> feedsPerStream = new ArrayList<Feed>();
			
			if(stream.equals("Twitter"))
				this.source = Source.Twitter;
			else if(stream.equals("Facebook"))
				this.source = Source.Facebook;
			else if(stream.equals("Flickr"))
				this.source = Source.Flickr;
			else if(stream.equals("GooglePlus"))
				this.source = Source.GooglePlus;
			else if(stream.equals("Instagram"))
				this.source = Source.Instagram;
			else if(stream.equals("Tumblr"))
				this.source = Source.Tumblr;
			else if(stream.equals("Topsy"))
				this.source = Source.Topsy;
			else if(stream.equals("Youtube"))
				this.source = Source.Youtube;
			
			Map<FeedType,Object> inputData = getData();
			
			for(FeedType feedType : inputData.keySet()){
				switch(feedType) {
				
				case ACCOUNT :
					@SuppressWarnings("unchecked")
					List<Account> sources = (List<Account>) inputData.get(feedType);
					for(Account source : sources){
						source.setSource(source.toString());
						String feedID = UUID.randomUUID().toString();
						AccountFeed sourceFeed = new AccountFeed(source, sinceDate, feedID);
						feedsPerStream.add(sourceFeed);
					}
					break;
				case KEYWORDS : 
					@SuppressWarnings("unchecked")
					List<String> keywords = (List<String>) inputData.get(feedType);
					for(String keyword : keywords){
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
	public Map<FeedType, Object> getData(){
		Map<FeedType,Object> inputDataPerType = new HashMap<FeedType,Object>();
		
		this.stream_config = config.getStreamInputConfig(source.toString());
	
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
		List<Account> sources = new ArrayList<Account>();
		
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
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}finally {
		        try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		    }
			
			for(String user : users) {
				Account account = new Account();
				account.setName(user);
				sources.add(account); 	
			}
			
		}
		
		//users by config file
		value = stream_config.getParameter(FOLLOWS);
		
		if (value != null && !value.equals("")) {
			
			if(value.contains(",")){
				
				String[] users = value.split(",");
			
				for(String user : users) {
					Account account = new Account();
					account.setName(user);
					sources.add(account); 	
				}
			}
			else{
				String user = value;
				Account account = new Account();
				account.setName(user);
				sources.add(account); 	
			}
		}
	
		if(!sources.isEmpty())
			inputDataPerType.put(FeedType.ACCOUNT, sources);
		
		//keywords
		List<String> keywords = new ArrayList<String>();
		
		value = stream_config.getParameter(KEYWORDS);
		if (value != null && !value.equals("")) {
			String[] tokens = value.split(",");
			
			for(String token : tokens) {
				keywords.add(token.toLowerCase());
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
