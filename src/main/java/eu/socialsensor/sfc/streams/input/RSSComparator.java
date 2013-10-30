package eu.socialsensor.sfc.streams.input;

import java.util.ArrayList;
import java.util.List;


import eu.socialsensor.framework.client.dao.ItemDAO;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.dysco.Entity;


/**
 * @brief : Class that compares RSS feeds with incoming dyscos
 * @author ailiakop
 * @email ailiakop@iti.gr
 */

public class RSSComparator {

	List<Item> rssItems;
	List<Entity> dyscoEntities;
	List<String> dyscoKeywords;

	
	int minimumScore = 9;
	int personScore = 6;
	int organizationScore = 4;
	int keywordScore = 2;
	
	public RSSComparator(ItemDAO itemDAO){
		
		rssItems = itemDAO.readItems();

		System.out.println();
		System.out.println("Number of RSS Items : #"+rssItems.size());
		System.out.println();
	}
	
	/**
	 * Returns the rss item that is most similar to the dysco
	 * @param dyscoEntities
	 * @param dyscoKeywords
	 * @return RSS Item
	 */
	public Item compare(List<Entity> dyscoEntities,List<String> dyscoKeywords){
		if(dyscoEntities != null)
			this.dyscoEntities = dyscoEntities;
		if(dyscoKeywords != null)
			this.dyscoKeywords = dyscoKeywords;
		
		int maxScore = minimumScore;
		Item selectedRSSItem = null;
		
		for(Item rssItem : rssItems){
			int temp = computeScore(rssItem);
			if(temp > maxScore){
				maxScore = temp;
				selectedRSSItem = rssItem;
			}
		}
		
		return selectedRSSItem;
	}
	/**
	 * Computes tha similarity score of the rss item to the dysco
	 * @param rssItem
	 * @return score
	 */
	public int computeScore(Item rssItem){
		int score = 0;
		List<String> checkedWords = new ArrayList<String>();
		
		if(dyscoEntities != null && rssItem.getEntities()!=null)
			for(Entity r_ent : rssItem.getEntities())
				for(Entity ent : dyscoEntities){
					if(checkedWords.contains(ent.getName()) || checkedWords.contains(r_ent.getName()))
						continue;
					if(r_ent.getName().toLowerCase().equals(ent.getName()) || r_ent.getName().toLowerCase().contains(" "+ent.getName()+" ") || ent.getName().contains(" "+r_ent.getName()+" ")){
						if(ent.getType().equals(Entity.Type.PERSON)){
							score+=personScore;
						}	
						else if(ent.getType().equals(Entity.Type.ORGANIZATION)){
							score+=organizationScore;
						}	
						else{
							score+=keywordScore;	
						}
						checkedWords.add(r_ent.getName());
						checkedWords.add(ent.getName());	
					}
				}
				
		if(dyscoKeywords != null && rssItem.getKeywords()!=null)
			for(String r_key : rssItem.getKeywords())
				for(String key : dyscoKeywords){
					if(checkedWords.contains(r_key) || checkedWords.contains(key))
						continue;
					if(r_key.toLowerCase().equals(key) || r_key.toLowerCase().contains(" "+key+" ") || key.contains(" "+r_key+" ")){
						score+=keywordScore;
						checkedWords.add(r_key);
						checkedWords.add(key);
					}
				}
		
		if(dyscoKeywords != null && rssItem.getEntities()!=null)
			for(Entity r_ent : rssItem.getEntities())
				for(String key : dyscoKeywords){
					if(checkedWords.contains(r_ent.getName()) || checkedWords.contains(key))
						continue;
					if(r_ent.getName().toLowerCase().equals(key) || r_ent.getName().toLowerCase().contains(" "+key+" ") || key.contains(" "+r_ent.getName()+" ")){
						score+=keywordScore;
						checkedWords.add(r_ent.getName());
						checkedWords.add(key);
					}
			
				}
		
		if(dyscoEntities != null && rssItem.getKeywords()!=null)
			for(Entity ent : dyscoEntities)
				for(String r_key : rssItem.getKeywords()){
					if(checkedWords.contains(ent.getName()) || checkedWords.contains(r_key))
						continue;
					if(ent.getName().equals(r_key.toLowerCase()) || ent.getName().contains(" "+r_key+" ") || r_key.toLowerCase().contains(" "+ent.getName()+" ")){
						if(ent.getType().equals(Entity.Type.PERSON)){
							score+=personScore;
						}
						else if(ent.getType().equals(Entity.Type.ORGANIZATION)){
							score+=organizationScore;
						}
						else{
							score+=keywordScore;
						}
						checkedWords.add(ent.getName());
						checkedWords.add(r_key);	
					}
				}
		
		return score;
	}
	
}
