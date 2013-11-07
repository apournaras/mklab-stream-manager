package eu.socialsensor.sfc.streams.input.FeedsCreatorImpl;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
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

/**
 * @brief Class for the creation of input feeds for a dysco using rss topics. 
 * The most representative keywords that will be used for the feed creation
 * are extracted from the most similar rss topics to the dysco after filtering,
 * comparing and ranking their content. The feeds are created by all the different 
 * combinations of the most representantive keywords of the dysco.
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class DynamicFeedsCreator implements FeedsCreator{
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
	
	private Set<Entity> mostImportantEntities = new HashSet<Entity>();
	
	Stopwords stopwords = new Stopwords();
	
	public DynamicFeedsCreator(Map<String,Set<String>> wordsToRSSItems){
		this.wordsToRSSItems = wordsToRSSItems;
	}
	
	/**
	 * Sets the best (most representative) keywords 
	 * @param topKeywords
	 */
	public void setTopKeywords(List<String> topKeywords){
		this.topKeywords = topKeywords;
	}
	/**
	 * Returns the best (most representative) keywords 
	 * @param topKeywords
	 * @return List of strings
	 */
	public List<String> getTopKeywords(){
		return topKeywords;
	}
	/**
	 * Returns the most important entities for the dysco
	 * 
	 * @return List of Entity
	 */
	public Set<Entity> getMostImportantEntities(){
		
		return mostImportantEntities;
	}
	
	/**
	 * Finds the most similar rss topics to the dysco
	 * @param dysco
	 * @return
	 */
	public List<String> extractSimilarRSSForDysco(Dysco dysco){
		this.dysco = dysco;
		
		dateOfRetrieval = dateUtil.addDays(dysco.getCreationDate(),-1);
		
		filterDyscoContent();
		extractDyscosAttributes();
		computeTopSimilarRSS();
		findMostSimilarRSS();
		
		if(mostSimilarRSSTopics.isEmpty())
			System.out.println("Dysco has no similar RSS Topic - Can't extract usefull keywords");
		
		return mostSimilarRSSTopics;
	}
	
	/**
	 * Sets the importance of its dysco word according to 
	 * its type and frequency in the dysco
	 */
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
	
	/**
	 * Computes the similarities scores of the rss topics
	 * to the dysco based on their common entities and keywords
	 */
	private void computeTopSimilarRSS(){
		Set<String> similarRSS = new HashSet<String>();
		
		//find most important entity
		int maxEntityScore = 0;
		for(Entity entity : entities){
			//find max score in entities
 			if(entity.getCont() > maxEntityScore){
				maxEntityScore = entity.getCont();
			}
		}
		for(Entity entity : entities){
			if(entity.getCont() == maxEntityScore)
				mostImportantEntities.add(entity);
		}
		
		if(!mostImportantEntities.isEmpty()){
			
			for(Entity imp_entity : mostImportantEntities){
				
				if(wordsToRSSItems.get(imp_entity.getName().toLowerCase())!=null)
					similarRSS.addAll(wordsToRSSItems.get(imp_entity.getName().toLowerCase()));
			}
				
			if(similarRSS.isEmpty())
				System.out.println("No similar RSS Items for important entities");
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
		
		if(similarRSS.isEmpty()){
			
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
	
	/**
	 * Finds the rss topics that have the maximum 
	 * similarity score to the dysco
	 */
	private void findMostSimilarRSS(){
		Integer maxScore = 0;
	
		for(String rss : rssTopicsScore.keySet()){
			if(rssTopicsScore.get(rss) > maxScore)
				maxScore = rssTopicsScore.get(rss);
		}
		mostSimilarRSSTopics.clear();
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
		
		String keyToRemove = null;
		for(String key : topKeywords)
			if(key.trim().equals(""))
				keyToRemove = key;
		
		if(keyToRemove != null)
			topKeywords.remove(keyToRemove);
		
		if(topKeywords.size() == 1){
			String feedID = UUID.randomUUID().toString();
			KeywordsFeed keywordsFeed = new KeywordsFeed(new Keyword(topKeywords.get(0).toLowerCase(),0.0f),dateOfRetrieval,feedID,dysco.getId());
			keywordsFeed.addQueryKeyword(topKeywords.get(0).toLowerCase());
			inputFeeds.add(keywordsFeed);
		}
		else{
			for(String key : topKeywords){
				
				int numOfWords = key.split(" ").length;
				if(numOfWords > 1 && numOfWords < 4){
					String feedID = UUID.randomUUID().toString();
					KeywordsFeed keywordsFeed = new KeywordsFeed(new Keyword(key,0.0f),dateOfRetrieval,feedID,dysco.getId());
					keywordsFeed.addQueryKeyword(key);
					inputFeeds.add(keywordsFeed);
					
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
		}
		
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
