package gr.iti.mklab.sfc.input;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import gr.iti.mklab.framework.common.domain.Location;
import gr.iti.mklab.framework.common.domain.Query;
import gr.iti.mklab.framework.common.domain.Account;
import gr.iti.mklab.framework.common.domain.dysco.Dysco;
import gr.iti.mklab.framework.common.domain.feeds.AccountFeed;
import gr.iti.mklab.framework.common.domain.feeds.Feed;
import gr.iti.mklab.framework.common.domain.feeds.KeywordsFeed;
import gr.iti.mklab.framework.common.domain.feeds.ListFeed;
import gr.iti.mklab.framework.common.domain.feeds.LocationFeed;
import gr.iti.mklab.framework.common.domain.feeds.Feed.FeedType;
import gr.iti.mklab.framework.common.util.DateUtil;

/**
 * @brief The class that is responsible for the creation of input feeds
 * from DySco content
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class DyscoInputReader implements InputReader {
	
	private Dysco dysco;
	
	private List<Feed> feeds = new ArrayList<Feed>();
	
	private Date date;
	private DateUtil dateUtil = new DateUtil();
	
	public DyscoInputReader(Dysco dysco) {
		this.dysco = dysco;
	}
	
	@Override
	public Map<FeedType, Object> getData() {
		Map<FeedType,Object> inputDataPerType = new HashMap<FeedType,Object>();
		
		this.date = dateUtil.addDays(dysco.getCreationDate(), -2);
		
		//standard for trending dysco 
		Set<String> queryKeywords = new HashSet<String>();
	
		List<Query> solrQueries = dysco.getSolrQueries();
		if(solrQueries != null) {
			for(Query solrQuery : solrQueries){
				String queryName = solrQuery.getName();
				queryKeywords.add(queryName);
			}
		}
		
		if(!queryKeywords.isEmpty()) {
			inputDataPerType.put(FeedType.KEYWORDS, queryKeywords);
		}
		
		return inputDataPerType;
		
	}

	@Override
	public Map<String, List<Feed>> createFeedsPerStream() {
		return null;
	}

	@Override
	public List<Feed> createFeeds() {
		
		Map<FeedType,Object> inputData = getData();
		
		for(FeedType feedType : inputData.keySet()) {
			switch(feedType) {
				case ACCOUNT :
					@SuppressWarnings("unchecked")
					List<Account> sources = (List<Account>) inputData.get(feedType);
					for(Account source : sources) {
						String feedID = UUID.randomUUID().toString();
						AccountFeed sourceFeed = new AccountFeed(source, date, feedID);
						feeds.add(sourceFeed);
					}
					break;
				case KEYWORDS : 
					@SuppressWarnings("unchecked")
					Set<String> keywords = (Set<String>) inputData.get(feedType);
					for(String keyword : keywords) {
						String feedID = UUID.randomUUID().toString();
						KeywordsFeed keywordsFeed = new KeywordsFeed(keyword, date, feedID);
						feeds.add(keywordsFeed);
					}
					break;
				case LOCATION :
					@SuppressWarnings("unchecked")
					List<Location> locations = (List<Location>) inputData.get(feedType);
					for(Location location : locations) {
						String feedID = UUID.randomUUID().toString();
						LocationFeed locationFeed = new LocationFeed(location, date, feedID);
						feeds.add(locationFeed);
					}
					break;
				case LIST :
					@SuppressWarnings("unchecked")
					List<String> lists = (List<String>) inputData.get(feedType);
					for(String list : lists) {
						String feedID = UUID.randomUUID().toString();
						ListFeed listFeed = new ListFeed(list, date, feedID);
						feeds.add(listFeed);
					}
				default:
					break;
			}
		}
		return feeds;
	}

}
