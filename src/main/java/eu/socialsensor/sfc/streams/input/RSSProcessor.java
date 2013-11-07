package eu.socialsensor.sfc.streams.input;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import eu.socialsensor.framework.client.dao.ItemDAO;
import eu.socialsensor.framework.client.dao.TopicDAO;
import eu.socialsensor.framework.common.domain.Topic;
import eu.socialsensor.framework.common.domain.dysco.Dysco;
import eu.socialsensor.framework.common.domain.dysco.Entity;

/**
 * @brief Class for processing the rss topics that will be used 
 * for keywords' extraction.
 * 
 * @author ailiakop
 * @email  ailiakop@iti.gr
 *
 */
public class RSSProcessor {
	private static final int TOP_KEYWORDS_NUMBER = 3;
	
	private Map<String,Topic> rssItems = new HashMap<String,Topic>();
	private Map<String,Set<String>> wordsToRSSItems = new HashMap<String,Set<String>>();
	private Map<Double,List<String>> rankedItems = new TreeMap<Double,List<String>>(Collections.reverseOrder());
	
	private List<String> mostSimilarRSSTopics = new ArrayList<String>();
	
	private TopicDAO topicDAO;
	
	public RSSProcessor(){
		super();
	}
	
	public void setRSSProcessor(TopicDAO topicDAO){
		this.topicDAO = topicDAO;
		
		List<Topic> topics = topicDAO.readTopicsByStatus();
		
		for(Topic rssTopic : topics){
			rssItems.put(rssTopic.getId(), rssTopic);
			rssTopic.setIsRead(true);
			topicDAO.updateTopic(rssTopic);
		}
	}
	
	/**
	 * Resets the processor
	 */
	public void resetRSSProcessor(){
		rssItems.clear();
		wordsToRSSItems.clear();
		rankedItems.clear();
		mostSimilarRSSTopics.clear();
	}
	
	/**
	 * Returns the mapping of a set of words to rss topics
	 * @return Map of the words to rss topics
	 */
	public Map<String,Set<String>> getWordsToRSSItems(){
		return wordsToRSSItems;
	}
	
	/**
	 * Sets the most similar rss topics found to the processor
	 * @param mostSimilarRSSTopics
	 */
	public void setMostSimilarRSSTopics(List<String> mostSimilarRSSTopics){
		this.mostSimilarRSSTopics = mostSimilarRSSTopics;
	}
	
	/**
	 * Returns the most similar rss topics found
	 * @return
	 */
	public List<String> getMostSimilarRSSTopics(){
		return mostSimilarRSSTopics;
	}
	
	/**
	 * Return the most frequest keywords found in the set of rss topics
	 * @return List of Strings
	 */
	private List<String> getTopKeywords(){
		List<String> topKeywords = new ArrayList<String>();
		
		for(Double score : rankedItems.keySet()){
			List<String> keys = rankedItems.get(score);
			for(String key : keys){
				topKeywords.add(key);
				if(topKeywords.size() >= TOP_KEYWORDS_NUMBER)
					return topKeywords;
			}
		}
		
		return topKeywords;
	}
	
	/**
	 * Creates the mapping of words found in rss items to rss items. 
	 * A word may be assigned to more than one rss items.
	 */
	public void processRSSItems(){
		for(Topic rssItem : rssItems.values()){
			for(Entity entity : rssItem.getEntities()){
				if(wordsToRSSItems.containsKey(entity.getName().toLowerCase())){
					wordsToRSSItems.get(entity.getName().toLowerCase()).add(rssItem.getId());
				}
				else{
					Set<String> rssTopics = new HashSet<String>();
					rssTopics.add(rssItem.getId());
					wordsToRSSItems.put(entity.getName().toLowerCase(), rssTopics);
				}
			}
			
			for(String keyword : rssItem.getKeywords()){
				if(wordsToRSSItems.containsKey(keyword)){
					wordsToRSSItems.get(keyword).add(rssItem.getId());
				}
				else{
					Set<String> rssTopics = new HashSet<String>();
					rssTopics.add(rssItem.getId());
					wordsToRSSItems.put(keyword, rssTopics);
				}
			}
		}
	}
	
	/**
	 * Computes the importance of a rss word for a dysco according to its 
	 * type (Entity/Keywords),the type of entity(Person/Location/Organization),
	 * whether it appears in dysco title or not and its frequency in the dysco content.
	 * Returns the best keywords as they are ranked after the computation. 
	 * @param similarRSSTopics
	 * @param dysco
	 * @return List of Strings
	 */
	public List<String> getTopKeywordsFromSimilarRSS(List <String> similarRSSTopics,Dysco dysco){
		Map<String,FeedKeyword> allWordsInRSS = new HashMap<String,FeedKeyword>();
		
		for(String mostSimilarRSS : similarRSSTopics){
			Topic rssTopic = rssItems.get(mostSimilarRSS);
			
			for(String keyword : rssTopic.getKeywords()){
				
				if(!allWordsInRSS.containsKey(keyword)){
					FeedKeyword f_keyword = new FeedKeyword(keyword);
					int num = 0;
					if(dysco.getTitle().contains(keyword))
						f_keyword.setIfExistsInTitle(true);
					if(dysco.getKeywords().contains(keyword))
						num++;
					for(Entity ent : dysco.getEntities()){
						if(ent.getName().toLowerCase().equals(keyword))
							num++;
					}
					f_keyword.setNumOfAppearancesInDysco(num);
					allWordsInRSS.put(keyword, f_keyword);
				}
				else{
					FeedKeyword f_keyword = allWordsInRSS.get(keyword);
					f_keyword.setNumOfAppearances(1);
					allWordsInRSS.put(keyword, f_keyword);
				}
			}
			for(Entity entity : rssTopic.getEntities()){
				if(!allWordsInRSS.containsKey(entity.getName().toLowerCase())){
					FeedKeyword f_keyword = new FeedKeyword(entity.getName().toLowerCase());
					int num = 0;
					if(dysco.getTitle().contains(entity.getName().toLowerCase()))
						f_keyword.setIfExistsInTitle(true);
					if(dysco.getKeywords().contains(entity.getName().toLowerCase()))
						num++;
					for(Entity ent : dysco.getEntities()){
						if(ent.getName().toLowerCase().equals(entity.getName().toLowerCase()))
							num++;
					}
					f_keyword.setIsEntity(true);
					if(entity.getType().equals(Entity.Type.ORGANIZATION) || entity.getType().equals(Entity.Type.ORGANIZATION))
						f_keyword.setIsPerson_Org(true);
					f_keyword.setNumOfAppearancesInDysco(num);
					allWordsInRSS.put(entity.getName().toLowerCase(), f_keyword);
				}
				else{
					FeedKeyword f_keyword = allWordsInRSS.get(entity.getName().toLowerCase());
					f_keyword.setNumOfAppearances(1);
					f_keyword.setIsEntity(true);
					if(entity.getType().equals(Entity.Type.ORGANIZATION) || entity.getType().equals(Entity.Type.ORGANIZATION))
						f_keyword.setIsPerson_Org(true);
					allWordsInRSS.put(entity.getName().toLowerCase(), f_keyword);
				}
			}
		}
	
		rankedItems.clear();
		for(FeedKeyword fk : allWordsInRSS.values()){
			double score = fk.computeScore();
			if(rankedItems.containsKey(score)){
				List<String> updatedItems = rankedItems.get(score);
				updatedItems.add(fk.getKeyword());
				rankedItems.put(score, updatedItems);
			}
			else{
				List<String> newItems = new ArrayList<String>();
				newItems.add(fk.getKeyword());
				rankedItems.put(score, newItems);
			}
		}
		
		return getTopKeywords();
	}
}
