package eu.socialsensor.sfc.streams.input.FeedsCreatorImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.socialsensor.framework.client.dao.ItemDAO;
import eu.socialsensor.framework.client.dao.impl.ItemDAOImpl;
import eu.socialsensor.framework.client.search.solr.SolrDyscoHandler;
import eu.socialsensor.framework.common.domain.Stopwords;
import eu.socialsensor.framework.common.domain.dysco.Dysco;
import eu.socialsensor.framework.common.domain.dysco.Entity;
import eu.socialsensor.sfc.streams.input.RSSProcessor;

public class RSSTopicFeedsCreator {
	private Map<String,Set<String>> wordsToRSSItems;
	private Map<String,Integer> dyscoAttributesWithScore = new HashMap<String,Integer>();
	private Map<String,Integer> rssTopicsScore = new HashMap<String,Integer>();
	
	private List<Entity> entities = new ArrayList<Entity>();
	private List<String> keywords = new ArrayList<String>();
	
	private static List<String> mostSimilarRSSTopics = new ArrayList<String>();
	
	private Dysco dysco;
	
	Stopwords stopwords = new Stopwords();
	
	public RSSTopicFeedsCreator(Map<String,Set<String>> wordsToRSSItems){
		this.wordsToRSSItems = wordsToRSSItems;
	}
	
	public List<String> createFeedsForDysco(Dysco dysco){
		this.dysco = dysco;
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
			dyscoAttributesWithScore.put(entity.getName().toLowerCase(), score);
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
		Entity mostImportantEntity = null;
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
	
	public static void main(String[] args) {
		SolrDyscoHandler dyscoHandler = SolrDyscoHandler.getInstance();
		ItemDAO itemDAO = new ItemDAOImpl("160.40.50.230","RSS_Topics_23_10","Topics");
		RSSProcessor rssProcessor = new RSSProcessor(itemDAO);
		rssProcessor.readTopics();
		rssProcessor.processRSSItems();
		RSSTopicFeedsCreator feedsCreator = new RSSTopicFeedsCreator(rssProcessor.getWordsToRSSItems());
		Dysco dysco = dyscoHandler.findDyscoLight("1e68b133-fb8b-49ea-8d70-c37c5e9ef5ee");
		feedsCreator.createFeedsForDysco(dysco);
		rssProcessor.setMostSimilarRSSTopics(mostSimilarRSSTopics);
		rssProcessor.computeKeywordsScore(dysco);
	}
}
