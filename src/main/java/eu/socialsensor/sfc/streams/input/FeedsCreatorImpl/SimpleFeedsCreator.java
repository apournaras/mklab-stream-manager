package eu.socialsensor.sfc.streams.input.FeedsCreatorImpl;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.common.domain.Keyword;
import eu.socialsensor.framework.common.domain.Stopwords;
import eu.socialsensor.framework.common.domain.dysco.Entity;
import eu.socialsensor.framework.common.domain.feeds.KeywordsFeed;
import eu.socialsensor.sfc.streams.input.FeedsCreator;

/**
 * @brief : Class the creation of feeds from any item with information included in keywords and entities
 * @author ailiakop
 * @email ailiakop@iti.gr
 */

public class SimpleFeedsCreator implements FeedsCreator{
	List<String> keywords = new ArrayList<String>();
	List<Entity> entities = new ArrayList<Entity>();
	
	List<String> keywordsToSearch = new ArrayList<String>();
	
	List<Feed> feeds = new ArrayList<Feed>();
	
	Date dateOfRetrieval;
	DateUtil dateUtil = new DateUtil();	
	
	Stopwords stopwords = new Stopwords();
	
	public SimpleFeedsCreator(Date date){
		
		this.dateOfRetrieval = dateUtil.addDays(date,-1);
	}
	
	/**
	 * Filters the content of keyword and entities
	 * @param keywords
	 * @param entities
	 */
	public void filterContent(List<String> keywords,List<Entity> entities){
		List<Entity> filteredEntities = new ArrayList<Entity>();
		List<String> filteredKeywords = new ArrayList<String>();
		
		//Filter entities
		if(entities != null){
			filteredEntities.addAll(entities);
			for(Entity entity : entities){
				int index = filteredEntities.indexOf(entity);
				if(entity.getName().contains("@")||entity.getName().contains("#") 
						|| stopwords.getStopwords().contains(entity.getName().toLowerCase())
						|| entity.getName().split(" ").length > 3){
					filteredEntities.remove(entity);
					continue;
				}
				if(entity.getName().contains("http")){
					String newEntity = entity.getName().substring(0,entity.getName().indexOf("http")-1);
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
			
			this.entities.addAll(filteredEntities);
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
			
			this.keywords.addAll(filteredKeywords);
		}
			
		/*System.out.println("Filtered entities : ");
		for(Entity fEntity : entities)
			System.out.print(fEntity.getName()+",");
		System.out.println();
		
		System.out.println("Filtered keywords : ");
		for(String fKeyword : keywords)
			System.out.print(fKeyword+",");
		System.out.println();
		*/
	}
	
	
	@Override
	public List<String> extractKeywords(){
		
		for(Entity ent : entities){
			if(!isKeywordAlreadyIn(ent.getName()))
				keywordsToSearch.add(ent.getName());
		}
		
		
		return keywordsToSearch;
	}
	
	private boolean isKeywordAlreadyIn(String key_1){
		String keyToRemove = null;
		for(String key : keywordsToSearch){
			if(key.contains(key_1)){
				return true;
			}
			else if(key_1.contains(key)){
				keyToRemove = key;
				break;
			}
		}
		
		if(keyToRemove != null){
			keywordsToSearch.remove(keyToRemove);
			keywordsToSearch.add(key_1);
			return true;
		}
		
		return false;
	}
	
	@Override
	public List<Feed> createFeeds(){
		List<String> tempKeywords = new ArrayList<String>();
		
		/*System.out.println("KEYWORDS :");
		for(String tkey : keywordsToSearch)
			System.out.println(tkey);*/
		
		String keyToRemove = null;
		for(String key : keywordsToSearch)
			if(key.equals(""))
				keyToRemove = key;
		
		if(keyToRemove != null)
			keywordsToSearch.remove(keyToRemove);
		
		for(String key : keywordsToSearch){
			
			int numOfWords = key.split(" ").length;
			if(numOfWords > 1 && numOfWords < 4){
				String feedID = UUID.randomUUID().toString();
				KeywordsFeed keywordsFeed = new KeywordsFeed(new Keyword(key,0.0f),dateOfRetrieval,feedID,null);
				keywordsFeed.addQueryKeyword(key);
				feeds.add(keywordsFeed);
				
			}
			else if(numOfWords >= 4){
				List<String> newKeys = new ArrayList<String>();
				for(int i=0;i<numOfWords;i++)
					newKeys.add(key.split(" ")[i]);
				
				keywordsToSearch.addAll(newKeys);
			}
		}
		
		//doublets
		for(int i = 0; i<keywordsToSearch.size(); i++){
			for(int j = i+1;j<keywordsToSearch.size();j++){
				if(!keywordsToSearch.get(i).equals(keywordsToSearch.get(j))){
					String feedID = UUID.randomUUID().toString();
					tempKeywords.add(keywordsToSearch.get(i));
					tempKeywords.add(keywordsToSearch.get(j));
					KeywordsFeed keywordsFeed = new KeywordsFeed(tempKeywords,dateOfRetrieval,feedID,null);
					keywordsFeed.addQueryKeywords(tempKeywords);
					
					feeds.add(keywordsFeed);
					
					tempKeywords.clear();
				}
			}
		}
		
		//triplets
		/*for(int i=0;i<keywordsToSearch.size();i++){
			for(int j=i+1;j<keywordsToSearch.size();j++){
				for(int k=j+1;k<keywordsToSearch.size();k++){
					if(!keywordsToSearch.get(i).equals(keywordsToSearch.get(j)) 
							&& !keywordsToSearch.get(i).equals(keywordsToSearch.get(k))
							&& !keywordsToSearch.get(j).equals(keywordsToSearch.get(k))){
						String feedID = UUID.randomUUID().toString();
						tempKeywords.add(keywordsToSearch.get(i));
						tempKeywords.add(keywordsToSearch.get(j));
						tempKeywords.add(keywordsToSearch.get(k));
						KeywordsFeed keywordsFeed = new KeywordsFeed(tempKeywords,dateOfRetrieval,feedID,null);
						keywordsFeed.addQueryKeywords(tempKeywords);
						
						feeds.add(keywordsFeed);
					
						tempKeywords.clear();
					}
						
				}
			}
		}*/
		
		return feeds;
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
