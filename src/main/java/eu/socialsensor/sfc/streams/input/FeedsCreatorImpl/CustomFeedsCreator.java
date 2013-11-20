package eu.socialsensor.sfc.streams.input.FeedsCreatorImpl;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.common.domain.Keyword;
import eu.socialsensor.framework.common.domain.Stopwords;
import eu.socialsensor.framework.common.domain.dysco.Dysco;
import eu.socialsensor.framework.common.domain.feeds.KeywordsFeed;
import eu.socialsensor.sfc.streams.input.FeedsCreator;

/**
 * @brief : Class the creation of feeds from custom dyscos
 * @author ailiakop
 * @email ailiakop@iti.gr
 */

public class CustomFeedsCreator implements FeedsCreator{
	Dysco dysco;
	
	Stopwords stopwords = new Stopwords();
	
	List<String> topKeywords = new ArrayList<String>();
	List<String> dyscoKeywords = new ArrayList<String>();
	List<String> dyscoTags = new ArrayList<String>();
	List<String> extractedKeywords = new ArrayList<String>();
	
	Date dateOfRetrieval;
	DateUtil dateUtil = new DateUtil();	
	
	public CustomFeedsCreator(Dysco dysco){
		this.dysco = dysco;
		this.dyscoKeywords = dysco.getKeywords();
		this.dyscoTags = dysco.getTags();
		
		this.dateOfRetrieval = dateUtil.addDays(dysco.getCreationDate(),-1);
		
	}
	
	@Override
	public List<String> extractFeedInfo(){
		filterContent();
		
		List<String> keysToRemove = new ArrayList<String>();
		
		for(String key : dyscoKeywords){
			if(key.equals("")){
				keysToRemove.add(key);
				continue;
			}
				
			if(key.contains("AND")){
				String [] newKeys = key.split("AND");
				for(int i=0;i<newKeys.length;i++)
					if(!topKeywords.contains(newKeys[i]))
						topKeywords.add(newKeys[i].trim());
			}
			else{
				if(!topKeywords.contains(key))
					topKeywords.add(key.trim());
			}
				
		}
		
		for(String r_key : keysToRemove){
			dyscoKeywords.remove(r_key);
		}
		keysToRemove.clear();
		for(String key : extractedKeywords){
			if(key.equals("")){
				keysToRemove.add(key);
				continue;
			}
			else
				topKeywords.add(key);
		}
		for(String r_key : keysToRemove){
			extractedKeywords.remove(r_key);
		}
			
//		System.out.println("TOP KEYWORDS : ");
//		for(String t_key : topKeywords)
//			System.out.println(t_key);
		
		return topKeywords;
	}
	@Override
	public List<Feed> createFeeds(){
		List<Feed> inputFeeds = new ArrayList<Feed>();
		List<String> tempKeywords = new ArrayList<String>();
		
		for(String key : dyscoKeywords){
			if(key.contains("AND")){
				String[] combinedKeywords = key.split("AND");
				for(int i=0;i<combinedKeywords.length;i++){
					tempKeywords.add(combinedKeywords[i].trim());
				}
				String feedID = UUID.randomUUID().toString();
				KeywordsFeed keywordsFeed = new KeywordsFeed(tempKeywords,dateOfRetrieval,feedID,dysco.getId());
				keywordsFeed.addQueryKeywords(tempKeywords);
				inputFeeds.add(keywordsFeed);
				tempKeywords.clear();
			}
			else{
				String feedID = UUID.randomUUID().toString();
				KeywordsFeed keywordsFeed = new KeywordsFeed(new Keyword(key,0.0f),dateOfRetrieval,feedID,dysco.getId());
				keywordsFeed.addQueryKeyword(key);
				inputFeeds.add(keywordsFeed);
			}
		}
		
		for(String key : extractedKeywords){
			String feedID = UUID.randomUUID().toString();
			KeywordsFeed keywordsFeed = new KeywordsFeed(new Keyword(key,0.0f),dateOfRetrieval,feedID,dysco.getId());
			keywordsFeed.addQueryKeyword(key);
			inputFeeds.add(keywordsFeed);
			
		}
		
		if(extractedKeywords.size() > 1) {
			//doublets
			for(int i = 0; i<extractedKeywords.size(); i++){
				for(int j = i+1;j<extractedKeywords.size();j++){
					if(!extractedKeywords.get(i).equals(extractedKeywords.get(j))){
						String feedID = UUID.randomUUID().toString();
						tempKeywords.add(extractedKeywords.get(i));
						tempKeywords.add(extractedKeywords.get(j));
						KeywordsFeed keywordsFeed = new KeywordsFeed(tempKeywords,dateOfRetrieval,feedID,dysco.getId());
						keywordsFeed.addQueryKeywords(tempKeywords);
						
						inputFeeds.add(keywordsFeed);
						tempKeywords.clear();
					}
				}
			}
		}
		
		if(extractedKeywords.size() > 2){
			//triplets
			for(int i=0;i<extractedKeywords.size();i++){
				for(int j=i+1;j<extractedKeywords.size();j++){
					for(int k=j+1;k<extractedKeywords.size();k++){
						if(!extractedKeywords.get(i).equals(extractedKeywords.get(j)) 
								&& !extractedKeywords.get(i).equals(extractedKeywords.get(k))
								&& !extractedKeywords.get(j).equals(extractedKeywords.get(k))){
							
							String feedID = UUID.randomUUID().toString();
							tempKeywords.add(extractedKeywords.get(i));
							tempKeywords.add(extractedKeywords.get(j));
							tempKeywords.add(extractedKeywords.get(k));
							KeywordsFeed keywordsFeed = new KeywordsFeed(tempKeywords,dateOfRetrieval,feedID,dysco.getId());
							keywordsFeed.addQueryKeywords(tempKeywords);
							
							inputFeeds.add(keywordsFeed);
							tempKeywords.clear();
						}
							
					}
				}
			}
		}
		
		
		return inputFeeds;
	}
	
	/**
	 * Filters dysco's content 
	 */
	private void filterContent(){
		for(String d_keyword : dyscoKeywords){
			d_keyword = d_keyword.toLowerCase();
			d_keyword = d_keyword.replaceAll("'s", "");
			d_keyword = d_keyword.replaceAll("'", "");
			d_keyword = d_keyword.replaceAll("[:.,?!;&'#]+","");
			d_keyword = d_keyword.replaceAll("\\s+", " ");
			
		}
		
		for(String d_tag : dyscoTags){
			d_tag = d_tag.toLowerCase();
			d_tag = d_tag.replaceAll("'s", "");
			d_tag = d_tag.replaceAll("'", "");
			d_tag = d_tag.replaceAll("[:.,?!;&'#]+","");
			d_tag = d_tag.replaceAll("\\s+", " ").trim();
			
			if(!extractedKeywords.contains(d_tag))
				extractedKeywords.add(d_tag);
		}
	}
	
	public class DateUtil
	{
	    public Date addDays(Date date, int days)
	    {
	        Calendar cal = Calendar.getInstance();
	        cal.setTime(date);
	        cal.add(Calendar.DATE, days); //minus number decrements the days
	        return cal.getTime();
	    }
	}
}
