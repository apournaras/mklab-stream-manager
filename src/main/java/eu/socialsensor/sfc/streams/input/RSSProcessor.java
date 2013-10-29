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
import eu.socialsensor.framework.common.domain.Topic;
import eu.socialsensor.framework.common.domain.dysco.Dysco;
import eu.socialsensor.framework.common.domain.dysco.Entity;

public class RSSProcessor {
	private static final int TOP_KEYWORDS_NUMBER = 3;
	
	private Map<String,Topic> rssItems = new HashMap<String,Topic>();
	private List<String> mostSimilarRSSTopics;
	private ItemDAO itemDAO;
	private Map<String,Set<String>> wordsToRSSItems = new HashMap<String,Set<String>>();
	Map<Double,List<String>> rankedItems = new TreeMap<Double,List<String>>(Collections.reverseOrder());
	
	public RSSProcessor(ItemDAO itemDAO){
		this.itemDAO = itemDAO;
	}
	
	public void readTopics(){
		for(Topic rssTopic : itemDAO.readTopics()){
			rssItems.put(rssTopic.getId(), rssTopic);
		}
	}
	
	public Map<String,Set<String>> getWordsToRSSItems(){
		return wordsToRSSItems;
	}
	
	public void setMostSimilarRSSTopics(List<String> mostSimilarRSSTopics){
		this.mostSimilarRSSTopics = mostSimilarRSSTopics;
	}
	
	public List<String> getMostSimilarRSSTopics(){
		return mostSimilarRSSTopics;
	}
	
	public Set<String> getTopKeywords(){
		Set<String> topKeywords = new HashSet<String>();
		
		for(List<String> keys : rankedItems.values()){
			for(String key : keys){
				topKeywords.add(key);
				if(topKeywords.size() > TOP_KEYWORDS_NUMBER)
					return topKeywords;
			}
		}
		
		return topKeywords;
	}
	
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
	
	public void computeKeywordsScore(Dysco dysco){
		Map<String,FeedKeyword> allWordsInRSS = new HashMap<String,FeedKeyword>();
		
		for(String mostSimilarRSS : mostSimilarRSSTopics){
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
		/*for(String keyword : allWordsInRSS.keySet()){
			System.out.println("Word : "+keyword);
			System.out.println("NumberOfApperances : "+allWordsInRSS.get(keyword).getNumOfAppearances());
			System.out.println("NumberOfApperances In dysco : "+allWordsInRSS.get(keyword).getNumOfAppearancesInDysco());
			System.out.println("IsEntity : "+allWordsInRSS.get(keyword).getIsEntity());
			System.out.println("IsPerson_Org : "+allWordsInRSS.get(keyword).getIsPerson_Org());
			System.out.println("ExistsInTitle : "+allWordsInRSS.get(keyword).getIfExistsInTitle());
		}
		*/
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
		
		for(Double score : rankedItems.keySet()){
			System.out.println("Score : "+ score+ " ----->  ");
			for(String rssTopic : rankedItems.get(score)){
				System.out.print(rssTopic+" ");
			}
			System.out.println();
		}
	}
}
