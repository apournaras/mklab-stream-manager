package eu.socialsensor.sfc.streams.input.FeedsCreatorImpl;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;


import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.common.domain.Keyword;
import eu.socialsensor.framework.common.domain.Stopwords;
import eu.socialsensor.framework.common.domain.dysco.Dysco;
import eu.socialsensor.framework.common.domain.dysco.Entity;
import eu.socialsensor.framework.common.domain.feeds.KeywordsFeed;
import eu.socialsensor.sfc.streams.input.FeedsCreator;

public class RSSTopicFeedsCreator implements FeedsCreator{
	private static final int MIN_RSS_THRESHOLD = 5;
	
	private Map<String,Set<String>> wordsToRSSItems;
	private Map<String,Integer> dyscoAttributesWithScore = new HashMap<String,Integer>();
	private Map<String,Integer> rssTopicsScore = new HashMap<String,Integer>();
	
	private List<Entity> entities = new ArrayList<Entity>();
	private List<String> keywords = new ArrayList<String>();
	private List<String> topKeywords = new ArrayList<String>();
	
	private static List<String> mostSimilarRSSTopics = new ArrayList<String>();
	
	private Dysco dysco;
	
	private Date dateOfRetrieval;
	private DateUtil dateUtil = new DateUtil();	
	
	private Entity mostImportantEntity = null;
	
	Stopwords stopwords = new Stopwords();
	
	public RSSTopicFeedsCreator(Map<String,Set<String>> wordsToRSSItems){
		this.wordsToRSSItems = wordsToRSSItems;
	}
	
	public void setTopKeywords(List<String> topKeywords){
		this.topKeywords = topKeywords;
	}
	
	public List<String> getTopKeywords(){
		return topKeywords;
	}
	
	public List<String> extractSimilarRSSForDysco(Dysco dysco){
		this.dysco = dysco;
		
		dateOfRetrieval = dateUtil.addDays(dysco.getCreationDate(),-1);
		
		filterDyscoContent();
		extractDyscosAttributes();
		computeTopSimilarRSS();
		findMostSimilarRSS();
		
		for(String rss : rssTopicsScore.keySet()){
			System.out.println("RSS : "+rss + " --- Score : "+rssTopicsScore.get(rss));
		}
		for(String rss : mostSimilarRSSTopics){
			System.out.println("Most Similar RSS : "+rss);
		}
		
		return mostSimilarRSSTopics;
	}
	
	private void extractDyscosAttributes(){
		for(Entity entity : entities){
			Integer score = entity.getCont();
			if(dysco.getTitle().contains(entity.getName().toLowerCase()))
				score *= 2;
			if(entity.getType().equals(Entity.Type.ORGANIZATION))
				score *= 2;
			if(entity.getType().equals(Entity.Type.PERSON))
				score *= 4;
			dyscoAttributesWithScore.put(entity.getName().toLowerCase(), score*entity.getCont());
		}
		
		for(String keyword : keywords){
			Integer score = 1;
			if(dysco.getTitle().contains(keyword.toLowerCase())){
				score *= 2;
			}
			dyscoAttributesWithScore.put(keyword.toLowerCase(), score);
		}
		
	}
	
	private void computeTopSimilarRSS(){
		Set<String> similarRSS = null;
		//find most important entity
		
		int maxEntityScore = 0;
		for(Entity entity : entities){
 			if(entity.getCont() > maxEntityScore){
				maxEntityScore = entity.getCont();
				mostImportantEntity = entity;
			}
		}
		
		if(mostImportantEntity != null){
			System.out.println("Most important entity : "+mostImportantEntity.getName());
			similarRSS = wordsToRSSItems.get(mostImportantEntity.getName().toLowerCase());
			if(similarRSS == null || similarRSS.isEmpty())
				System.out.println("No similar RSS Items for this entity");
			else{
				for(Entity entity : entities){
					if(wordsToRSSItems.containsKey(entity.getName().toLowerCase())){
						Integer score = dyscoAttributesWithScore.get(entity.getName().toLowerCase());
						for(String rss : similarRSS){
							if(wordsToRSSItems.get(entity.getName().toLowerCase()).contains(rss)){
								if(rssTopicsScore.containsKey(rss)){
									rssTopicsScore.put(rss, (rssTopicsScore.get(rss)+1)*score);
								}
								else{
									rssTopicsScore.put(rss, score);
								}
							}
							
						}
					}
				}
				
				for(String keyword : keywords){
					if(wordsToRSSItems.containsKey(keyword.toLowerCase())){
						Integer score = dyscoAttributesWithScore.get(keyword.toLowerCase());
						for(String rss : similarRSS){
							if(wordsToRSSItems.get(keyword.toLowerCase()).contains(rss)){
								if(rssTopicsScore.containsKey(rss)){
									rssTopicsScore.put(rss, (rssTopicsScore.get(rss)+1)*score);
								}
								else{
									rssTopicsScore.put(rss, score);
								}
							}
							
						}
					}
				}
			}
		}
		
		if(similarRSS == null || similarRSS.isEmpty()){
			for(Entity entity : entities){
				
				if(wordsToRSSItems.containsKey(entity.getName().toLowerCase())){
					Integer score = dyscoAttributesWithScore.get(entity.getName().toLowerCase());
					similarRSS = wordsToRSSItems.get(entity.getName().toLowerCase());
					for(String rss : similarRSS){
						if(rssTopicsScore.containsKey(rss)){
							rssTopicsScore.put(rss, (rssTopicsScore.get(rss)+1)*score);
						}
						else{
							rssTopicsScore.put(rss, score);
						}
					}
				}
			}
			
			for(String keyword : keywords){
				
				if(wordsToRSSItems.containsKey(keyword.toLowerCase())){
					Integer score = dyscoAttributesWithScore.get(keyword.toLowerCase());
					similarRSS = wordsToRSSItems.get(keyword.toLowerCase());
					for(String rss : similarRSS){
						if(rssTopicsScore.containsKey(rss)){
							rssTopicsScore.put(rss, (rssTopicsScore.get(rss)+1)*score);
						}
						else{
							rssTopicsScore.put(rss, score);
						}
					}
				}
			}
			
			boolean isSimilar = false;
			for(Integer score : rssTopicsScore.values()){
				if(score >= MIN_RSS_THRESHOLD)
					isSimilar = true;
			}
			
			if(!isSimilar)
				rssTopicsScore.clear();
		}
	}
	
	private void findMostSimilarRSS(){
		Integer maxScore = 0;
	
		for(String rss : rssTopicsScore.keySet()){
			if(rssTopicsScore.get(rss) > maxScore)
				maxScore = rssTopicsScore.get(rss);
		}
		
		for(Map.Entry<String, Integer> entry : rssTopicsScore.entrySet()){
			if(entry.getValue() == maxScore){
				mostSimilarRSSTopics.add(entry.getKey());
			}
		}
	}
	
	/**
	 * Filters dysco's content 
	 */
	private void filterDyscoContent(){
		
		List<Entity> filteredEntities = new ArrayList<Entity>();
		List<String> filteredKeywords = new ArrayList<String>();
		
		//Filter entities
		if(dysco.getEntities() != null){
			filteredEntities.addAll(dysco.getEntities());
			for(Entity entity : dysco.getEntities()){
				
				int r_entity = -1;
				for(Entity f_entity : filteredEntities){
					if(f_entity.getName().contains(entity.getName()) && !f_entity.getName().equals(entity.getName())){
						r_entity = filteredEntities.indexOf(entity);
						break;
					}
					else if(entity.getName().contains(f_entity.getName()) && !f_entity.getName().equals(entity.getName())){
						r_entity = filteredEntities.indexOf(f_entity);;
						break;
					}
						
				}
				
				if(r_entity != -1){
					filteredEntities.remove(r_entity);
				}
				
				int index = filteredEntities.indexOf(entity);
				if(index != -1){
					if(entity.getName().contains("#") 
							|| Stopwords.isStopword(entity.getName().toLowerCase())
							|| entity.getName().split(" ").length > 3){
						filteredEntities.remove(entity);
						continue;
					}
					if(entity.getName().contains("http")){
						String newEntity = entity.getName().substring(0,entity.getName().indexOf("http")-1);
						filteredEntities.get(index).setName(newEntity);
					}
					if(entity.getName().contains("@")){
						String newEntity = entity.getName().replace("@", "");
						filteredEntities.get(index).setName(newEntity);
					}
					
						
					filteredEntities.get(index).setName(filteredEntities.get(index).getName().toLowerCase());
					filteredEntities.get(index).setName(filteredEntities.get(index).getName().replaceAll("'s", ""));
					filteredEntities.get(index).setName(filteredEntities.get(index).getName().replaceAll("\\(", ""));
					filteredEntities.get(index).setName(filteredEntities.get(index).getName().replaceAll("\\)", ""));
					filteredEntities.get(index).setName(filteredEntities.get(index).getName().replaceAll("'", ""));
					filteredEntities.get(index).setName(filteredEntities.get(index).getName().replaceAll("[:.,?!;&'#-]+",""));
					filteredEntities.get(index).setName(filteredEntities.get(index).getName().replaceAll("\\s+", " "));
	       		 	
				}
			}
			
			entities.addAll(filteredEntities);
		}
			
		//Filter keywords
		if(dysco.getKeywords() != null){
			filteredKeywords.addAll(dysco.getKeywords());
			for(String key : dysco.getKeywords()){
				int index = filteredKeywords.indexOf(key);
			
				if(key.contains("@")||key.contains("#") 
						|| stopwords.is(key)
						|| key.split(" ").length > 3){
					filteredKeywords.remove(key);
					continue;
				}
				if(key.contains("http")){
					String newKey = key.substring(0,key.indexOf("http"));
					filteredKeywords.get(index).replace(filteredKeywords.get(index), newKey);
				}
				
				filteredKeywords.get(index).toLowerCase();
				filteredKeywords.get(index).replaceAll("'s", "");
				filteredKeywords.get(index).replaceAll("\\(", "");
				filteredKeywords.get(index).replaceAll("\\)", "");
				filteredKeywords.get(index).replaceAll("'", "");
				filteredKeywords.get(index).replaceAll("[:.,?!;&'#]+-","");
				filteredKeywords.get(index).replaceAll("\\s+", " ");
       		 	
			}
			
			keywords.addAll(filteredKeywords);
		}
			
	}
	
	
	
	@Override
	public List<String> extractKeywords(){
		
		return topKeywords;
	}
	
	@Override
	public List<Feed> createFeeds(){
		List<Feed> inputFeeds = new ArrayList<Feed>();
		List<String> tempKeywords = new ArrayList<String>();
		
		//topKeywords = rssProcessor.getTopKeywords();
		
		String keyToRemove = null;
		for(String key : topKeywords)
			if(key.equals(""))
				keyToRemove = key;
		
		if(keyToRemove != null)
			topKeywords.remove(keyToRemove);
		
		if(topKeywords.isEmpty() && mostImportantEntity!=null){
			String feedID = UUID.randomUUID().toString();
			KeywordsFeed keywordsFeed = new KeywordsFeed(new Keyword(mostImportantEntity.getName().toLowerCase(),0.0f),dateOfRetrieval,feedID,dysco.getId());
			keywordsFeed.addQueryKeyword(mostImportantEntity.getName().toLowerCase());
			inputFeeds.add(keywordsFeed);
		}
			
		
		for(String key : topKeywords){
			
			int numOfWords = key.split(" ").length;
			if(numOfWords > 1 && numOfWords < 4){
				String feedID = UUID.randomUUID().toString();
				KeywordsFeed keywordsFeed = new KeywordsFeed(new Keyword(key,0.0f),dateOfRetrieval,feedID,dysco.getId());
				keywordsFeed.addQueryKeyword(key);
				inputFeeds.add(keywordsFeed);
				
			}
			else if(numOfWords >= 4){
				List<String> newKeys = new ArrayList<String>();
				for(int i=0;i<numOfWords;i++)
					newKeys.add(key.split(" ")[i]);
				
				topKeywords.addAll(newKeys);
			}
		}
		
		//doublets
		for(int i = 0; i<topKeywords.size(); i++){
			for(int j = i+1;j<topKeywords.size();j++){
				if(!topKeywords.get(i).equals(topKeywords.get(j))){
					String feedID = UUID.randomUUID().toString();
					tempKeywords.add(topKeywords.get(i));
					tempKeywords.add(topKeywords.get(j));
					KeywordsFeed keywordsFeed = new KeywordsFeed(tempKeywords,dateOfRetrieval,feedID,dysco.getId());
					keywordsFeed.addQueryKeywords(tempKeywords);
					
					inputFeeds.add(keywordsFeed);
					
					tempKeywords.clear();
				}
			}
		}
		
		//triplets
		for(int i=0;i<topKeywords.size();i++){
			for(int j=i+1;j<topKeywords.size();j++){
				for(int k=j+1;k<topKeywords.size();k++){
					if(!topKeywords.get(i).equals(topKeywords.get(j)) 
							&& !topKeywords.get(i).equals(topKeywords.get(k))
							&& !topKeywords.get(j).equals(topKeywords.get(k))){
						String feedID = UUID.randomUUID().toString();
						tempKeywords.add(topKeywords.get(i));
						tempKeywords.add(topKeywords.get(j));
						tempKeywords.add(topKeywords.get(k));
						KeywordsFeed keywordsFeed = new KeywordsFeed(tempKeywords,dateOfRetrieval,feedID,dysco.getId());
						keywordsFeed.addQueryKeywords(tempKeywords);
						
						inputFeeds.add(keywordsFeed);
					
						tempKeywords.clear();
					}
						
				}
			}
		}
		System.out.println("Created feeds : ");
		for(Feed feed : inputFeeds)
			System.out.println(((KeywordsFeed) feed).toJSONString());
		return inputFeeds;
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
	
	public static void main(String[] args) {
		
	}
}
