package eu.socialsensor.sfc.streams.input.FeedsCreatorImpl;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.Keyword;
import eu.socialsensor.framework.common.domain.Stopwords;
import eu.socialsensor.framework.common.domain.dysco.Dysco;
import eu.socialsensor.framework.common.domain.dysco.Entity;
import eu.socialsensor.framework.common.domain.dysco.Ngram;
import eu.socialsensor.framework.common.domain.feeds.KeywordsFeed;
import eu.socialsensor.sfc.streams.input.FeedsCreator;
import eu.socialsensor.sfc.streams.input.RSSComparator;

/**
 * @brief : Class the creation of feeds from trending dyscos
 * @author ailiakop
 * @email ailiakop@iti.gr
 */

public class TrendingFeedsCreator implements FeedsCreator{
	Dysco dysco;
	Item rssItem;
	Stopwords stopwords = new Stopwords();
	
	List<String> topKeywords = new ArrayList<String>();
	
	Map<String,Integer> commonKeywords = new HashMap<String,Integer>();
	
	RSSComparator comparator;
	
	Date dateOfRetrieval;
	DateUtil dateUtil = new DateUtil();	
	
	int numberOfKeywords = 3;
	
	public TrendingFeedsCreator(Dysco dysco,RSSComparator comparator){
		this.dysco = dysco;
		this.comparator = comparator;
		
		dateOfRetrieval = dateUtil.addDays(dysco.getCreationDate(),-1);
		
		for(Ngram ngram : dysco.getNgrams()){
			System.out.println("ngram : "+ngram.getTerm());
		}
		
	}
	
	public void setRSSComparator(RSSComparator comparator){
		this.comparator = comparator;
	}
	
	/**
	 * Filters dysco's content 
	 */
	private void filterContent(List<Entity> entities, List<String> keywords){
		List<Entity> filteredEntities = new ArrayList<Entity>();
		List<String> filteredKeywords = new ArrayList<String>();
		
		//Filter entities
		if(entities != null){
			filteredEntities.addAll(entities);
			for(Entity entity : entities){
				int index = filteredEntities.indexOf(entity);
				if(entity.getName().contains("#") 
						|| stopwords.getStopwords().contains(entity.getName().toLowerCase())
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
			entities.clear();
			entities.addAll(filteredEntities);
		}
			
		//Filter keywords
		if(keywords != null){
			filteredKeywords.addAll(keywords);
			for(String key : keywords){
				int index = filteredKeywords.indexOf(key);
				if(key.contains("@")||key.contains("#") 
						|| stopwords.getStopwords().contains(key.toLowerCase())
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
			keywords.clear();
			keywords.addAll(filteredKeywords);
		}
			
		
		/*System.out.println("Filtered entities : ");
		for(Entity fEntity : entities)
			System.out.print(fEntity.getName()+" ");
		System.out.println();
		
		System.out.println("Filtered keywords : ");
		for(String fKeyword : keywords)
			System.out.print(fKeyword+" ");
		System.out.println();
		*/
	}
	@Override
	public List<String> extractFeedInfo(){
		filterContent(dysco.getEntities(),dysco.getKeywords());
		long t1 = System.currentTimeMillis();
		rssItem = comparator.compare(dysco.getEntities(), dysco.getKeywords());
		long t2 = System.currentTimeMillis();
		System.out.println("Time to find the most similar RSS Item : "+(t2-t1)+" msecs");
		if(rssItem == null){
			System.out.println("No similar RSS Item found");
			return topKeywords;
		}
		System.out.println("I have selected rssItem with id :"+rssItem.getId()+" with score : #"+comparator.computeScore(rssItem));
		filterContent(rssItem.getEntities(),rssItem.getKeywords());
			
		detectCommonKeywords();
		
		if(commonKeywords.isEmpty() || commonKeywords == null){
			System.err.println("No common keywords - Something went wrong");
			return topKeywords;
		}
		
		for(int i=0;i<numberOfKeywords;i++){
			 int maxScore = 0;
			 String selectedKey = null;
			 for(Entry<String,Integer> entry : commonKeywords.entrySet()){
				 if(entry.getValue()>maxScore){
					 maxScore = entry.getValue();
					 selectedKey = entry.getKey();
				 }
			 }
			 
			 if(selectedKey != null){
				 topKeywords.add(selectedKey);
				 commonKeywords.remove(selectedKey);
			 }
			 
		 }
		return topKeywords;
	}
	@Override
	public List<Feed> createFeeds(){
		List<Feed> inputFeeds = new ArrayList<Feed>();
		List<String> tempKeywords = new ArrayList<String>();
		
//		System.out.println("TOP KEYWORDS :");
//		for(String tkey : topKeywords)
//			System.out.println(tkey);
//		
		String keyToRemove = null;
		for(String key : topKeywords)
			if(key.equals(""))
				keyToRemove = key;
		
		if(keyToRemove != null)
			topKeywords.remove(keyToRemove);
		
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
		
		return inputFeeds;
	}
	
	/**
	 * Detects common keywords - entities and counts their number of appearances
	 * 
	 */
	private void detectCommonKeywords(){
		Map<String,Integer> selectedKeywords = new HashMap<String,Integer>();
		
		int personScore = 6;
		int organizationScore = 4;
		int keywordScore = 2;
		
		//Dysco entities - RSS entities
		if(dysco.getEntities() != null && rssItem.getEntities()!=null)
			for(Entity r_ent : rssItem.getEntities())
				for(Entity ent : dysco.getEntities()){
					//System.out.println("Comparing dysco entity : "+ent+" with rss entity : "+r_ent);
					if(r_ent.getName().toLowerCase().equals(ent.getName()) || ent.getName().contains(r_ent.getName().toLowerCase())){
						if(ent.getType().equals(Entity.Type.PERSON)){
							if(selectedKeywords.containsKey(ent.getName()) && selectedKeywords.get(ent.getName())<personScore){
								//System.out.println("Update  selected keywords : "+ent.getName()+" with score : "+personScore);
								selectedKeywords.remove(ent.getName());
								selectedKeywords.put(ent.getName(), personScore);
							}
							else if(!selectedKeywords.containsKey(ent.getName())){
								//System.out.println("Add to selected keywords : "+ent.getName()+" with score : "+personScore);
								selectedKeywords.put(ent.getName(), personScore);
							}
						}	
						else if(ent.getType().equals(Entity.Type.ORGANIZATION)){
							if(selectedKeywords.containsKey(ent.getName()) && selectedKeywords.get(ent.getName())<organizationScore){
								//System.out.println("Update  selected keywords : "+ent.getName()+" with score : "+organizationScore);
								selectedKeywords.remove(ent.getName());
								selectedKeywords.put(ent.getName(), organizationScore);
							}
							else if(!selectedKeywords.containsKey(ent.getName())){
								//System.out.println("Add to selected keywords : "+ent.getName()+" with score : "+organizationScore);
								selectedKeywords.put(ent.getName(), organizationScore);
							}
						}
						else{
							if(!selectedKeywords.containsKey(ent.getName())){
								//System.out.println("Add to selected keywords : "+ent.getName()+" with score : "+keywordScore);
								selectedKeywords.put(ent.getName(), keywordScore);
							}	
						}
							
					}
					else if(r_ent.getName().toLowerCase().contains(ent.getName())){
						if(!selectedKeywords.containsKey(ent.getName())){
							//System.out.println("Add to selected keywords : "+r_ent.getName()+" with score : "+keywordScore);
							selectedKeywords.put(r_ent.getName(), keywordScore);	
						}
					}
					
				}
					
		//Dysco Keywords - RSS Keywords
		if(dysco.getKeywords() != null && rssItem.getKeywords()!=null)
			for(String r_key : rssItem.getKeywords())
				for(String key : dysco.getKeywords()){
					//System.out.println("Comparing dysco key : "+key+" with rss key : "+r_key);
					if(r_key.toLowerCase().equals(key) || r_key.toLowerCase().contains(key)){
						if(!selectedKeywords.containsKey(r_key)){
							//System.out.println("Add to selected keywords : "+r_key+" with score : "+keywordScore);
							selectedKeywords.put(r_key, keywordScore);	
						}
					}
					else if(key.contains(r_key.toLowerCase())){
						if(!selectedKeywords.containsKey(key)){
							//System.out.println("Add to selected keywords : "+key+" with score : "+keywordScore);
							selectedKeywords.put(key, keywordScore);	
						}
					}
						
				}
			
		//Dysco keywords - RSS Entities
		if(dysco.getKeywords() != null && rssItem.getEntities()!=null)
			for(Entity r_ent : rssItem.getEntities())
				for(String key : dysco.getKeywords()){
					//System.out.println("Comparing dysco key : "+key+" with rss entity : "+r_ent);
					if(r_ent.getName().toLowerCase().equals(key) || r_ent.getName().toLowerCase().contains(key)){	
						if(!selectedKeywords.containsKey(r_ent.getName())){
							//System.out.println("Add to selected keywords : "+r_ent.getName()+" with score : "+keywordScore);
							selectedKeywords.put(r_ent.getName(), keywordScore);	
						}
					}
					else if(key.contains(r_ent.getName().toLowerCase())){
						if(!selectedKeywords.containsKey(key)){
							//System.out.println("Add to selected keywords : "+key+" with score : "+keywordScore);
							selectedKeywords.put(key, keywordScore);	
						}
					}
			
				}
					
		//Dysco entities - RSS Keywords
		if(dysco.getEntities() != null && rssItem.getKeywords()!=null)
			for(Entity ent : dysco.getEntities())
				for(String r_key : rssItem.getKeywords()){
					//System.out.println("Comparing dysco entity : "+ent+" with rss key : "+r_key);
					if(ent.getName().equals(r_key.toLowerCase()) || ent.getName().contains(r_key.toLowerCase())){
						if(ent.getType().equals(Entity.Type.PERSON)){
							if(selectedKeywords.containsKey(ent.getName()) && selectedKeywords.get(ent.getName())<personScore){
								//System.out.println("Update  selected keywords : "+ent.getName()+" with score : "+personScore);
								selectedKeywords.remove(ent.getName());
								selectedKeywords.put(ent.getName(), personScore);
							}
							else if(!selectedKeywords.containsKey(ent.getName())){
								//System.out.println("Add to selected keywords : "+ent.getName()+" with score : "+personScore);
								selectedKeywords.put(ent.getName(), personScore);
							}
						}
						else if(ent.getType().equals(Entity.Type.ORGANIZATION)){
							if(selectedKeywords.containsKey(ent.getName()) && selectedKeywords.get(ent.getName())<organizationScore){
								//System.out.println("Update  selected keywords : "+ent.getName()+" with score : "+organizationScore);
								selectedKeywords.remove(ent.getName());
								selectedKeywords.put(ent.getName(), organizationScore);
							}
							else if(!selectedKeywords.containsKey(ent.getName())){
								//System.out.println("Add to selected keywords : "+ent.getName()+" with score : "+organizationScore);
								selectedKeywords.put(ent.getName(), organizationScore);
							}
						}
						else{
							if(!selectedKeywords.containsKey(ent.getName())){
								//System.out.println("Add to selected keywords : "+ent.getName()+" with score : "+keywordScore);
								selectedKeywords.put(ent.getName(), keywordScore);
							}
						}
								
					}
					else if(r_key.toLowerCase().contains(ent.getName())){
						if(!selectedKeywords.containsKey(r_key)){
							//System.out.println("Add to selected keywords : "+r_key+" with score : "+keywordScore);
							selectedKeywords.put(r_key, keywordScore);	
						}
					}
				}
					
		
		/*System.out.println("Selected Keywords : ");
		for(Entry<String,Integer> entry : selectedKeywords.entrySet()){
			System.out.println(entry.getKey()+" with value : "+entry.getValue());
		}*/
		commonKeywords.putAll(selectedKeywords);
		
		for(Entry<String,Integer> entry : selectedKeywords.entrySet()){
			for(Entry<String,Integer> d_entry : selectedKeywords.entrySet()){
				//System.out.println("Comparing :"+entry.getKey()+" with "+d_entry.getKey());
				if(entry.getKey().equals(d_entry.getKey()))
					continue;
				if(entry.getKey().contains(d_entry.getKey())){
					//System.out.println("remove "+d_entry.getKey());
					commonKeywords.remove(d_entry.getKey());
				}
				else if(d_entry.getKey().contains(entry.getKey())){
					//System.out.println("remove "+entry.getKey());
					commonKeywords.remove(entry.getKey());
				}
				
			}
		}
		
//		System.out.println("Common Keywords : ");
//		for(Entry<String,Integer> entry : commonKeywords.entrySet()){
//			System.out.println(entry.getKey()+" with value : "+entry.getValue());
//		}
	}
	
	/**
	 * Returns true if the selected rss item is already searched with the wrappers
	 * @return
	 */
	public boolean isAlreadySearched(){
		if(rssItem != null){
			if(rssItem.getIsSearched())
				return true;
			else{
				rssItem.setIsSearched(true);
				return false;
			}
		}
		
		return false;
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
