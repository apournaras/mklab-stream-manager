package gr.iti.mklab.sfc.input;

import java.io.BufferedReader;
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

import gr.iti.mklab.framework.common.domain.Configuration;
import gr.iti.mklab.framework.common.domain.NewsFeedSource;
import gr.iti.mklab.framework.common.domain.feeds.Feed;
import gr.iti.mklab.framework.common.domain.feeds.URLFeed;
import gr.iti.mklab.framework.common.domain.feeds.Feed.FeedType;


/**
 * Class responsible for the creation of input feeds from a txt file.
 * @author ailiakop
 * @email ailiakop@iti.gr
 */
public class FileInputReader implements InputReader{
	protected static final String DATE = "since";
	protected static final String PATH = "path";
	protected static final String TYPE = "feedType";
	
	private InputConfiguration config;
	
	private Set<String> streams = null;
	
	private NewsFeedSource streamType = null;
	
	private Configuration stream_config = null;
	
	private Map<String,List<Feed>> feeds = new HashMap<String,List<Feed>>();
	
	public FileInputReader(InputConfiguration config) {
		this.config = config;
		streams = config.getStreamInputIds();
	}
	
	@Override
	public Map<FeedType, Object> getData() {
		
		Map<FeedType,Object> inputDataPerType = new HashMap<FeedType,Object>();
		Map<String, String> inputLines = new HashMap<String, String>();
		String path = stream_config.getParameter(FileInputReader.PATH);
		String type = stream_config.getParameter(FileInputReader.TYPE);
		
		System.out.println("GET DATA FROM " + path + " WITH TYPE " + type);
		
		String line;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(path));
			line = reader.readLine();
			while (line != null) {
				String[] parts = line.split("\t");
				if(parts.length == 2) {
					inputLines.put(parts[0], parts[1]);
				}
				else {
					System.out.println("Parts size: " + parts.length);
				}
	            line = reader.readLine();
	        }
			
			reader.close();
		} catch (IOException e) {
	e.printStackTrace();
		}
		
		if(type.equals("url")) {
			inputDataPerType.put(FeedType.URL, inputLines);
		}
		
		return inputDataPerType;
	}
	
	@Override
	public Map<String,List<Feed>> createFeedsPerStream() {
		for(String stream : streams){
			List<Feed> feedsPerStream = new ArrayList<Feed>();
			
			if(stream.equals("RSS"))
				this.streamType = NewsFeedSource.RSS;
			
			this.stream_config = config.getStreamInputConfig(streamType.toString());
			
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
			String since = stream_config.getParameter(FileInputReader.DATE);
			Date dateToRetrieve = null;
			if(since != null) {
				try {
					dateToRetrieve = (Date) formatter.parse(since);
					
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
			
			Map<FeedType, Object> inputData = getData();
			
			for(FeedType feedType : inputData.keySet()) {
				switch(feedType) {
					case URL :
						@SuppressWarnings("unchecked")
						Map<String, String> urlsMap = (Map<String, String>) inputData.get(feedType);
						
						for(String url : urlsMap.keySet()) {
							String listId = urlsMap.get(url);
							
							String feedId = UUID.randomUUID().toString();
							URLFeed feed = new URLFeed(url, dateToRetrieve, feedId);
							feed.setLabel(listId);
					
							feedsPerStream.add(feed);
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
	
	public List<Feed> createFeeds(){
		return null;
	}
	
	public Map<String,Set<String>> getUsersToLists(){
		return null;
	}

}
