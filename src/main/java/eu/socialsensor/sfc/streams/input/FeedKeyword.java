package eu.socialsensor.sfc.streams.input;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import eu.socialsensor.framework.common.domain.JSONable;

public class FeedKeyword implements JSONable{
	
	private final static Double DF = 1.0;
	private final static Double AP_TITLE = 0.8;
	private final static Double PERSON_ORG = 0.4;
	private final static Double ENTITY = 0.2;
	private final static Double AP_DYSCO = 0.6;
	
	private String keyword;	
	private boolean isEntity = false;
	private boolean isPerson_Org = false;
	private boolean existsInTitle = false;
	private Integer numOfAppearances = 1;
	private Integer numOfAppearancesInDysco = 0;
	
	private Double score = 0.0;
	
	public FeedKeyword(){
		super();
	}
	
	public FeedKeyword(String keyword){
		super();
		
		this.keyword = keyword;
	}
	
	public String getKeyword(){
		return this.keyword;
	}
	
	public void setKeyword(String keyword){
		this.keyword = keyword;
	}
	
	public boolean getIsEntity(){
		return isEntity;
	}
	
	public void setIsEntity(boolean isEntity){
		this.isEntity = isEntity;
	}
	
	public boolean getIsPerson_Org(){
		return isPerson_Org;
	}
	
	public void setIsPerson_Org(boolean isPerson_Org){
		this.isPerson_Org = isPerson_Org;
	}
	
	public boolean getIfExistsInTitle(){
		return existsInTitle;
	}
	
	public void setIfExistsInTitle(boolean existsInTitle){
		this.existsInTitle = existsInTitle;
	}
	
	public Integer getNumOfAppearances(){
		return this.numOfAppearances;
	}
	
	public void setNumOfAppearances(Integer numOfAppearances){
		this.numOfAppearances += numOfAppearances;
	}
	
	public Integer getNumOfAppearancesInDysco(){
		return this.numOfAppearancesInDysco;
	}
	
	public void setNumOfAppearancesInDysco(Integer numOfAppearancesInDysco){
		this.numOfAppearancesInDysco += numOfAppearancesInDysco;
	}
	
	public Double computeScore(){
		int title = 0 ;
		int entity = 0;
		int person_org = 0;
		
		if(existsInTitle)
			title = 1;
		
		if(isPerson_Org)
			person_org = 1;
		
		if(isEntity)
			entity = 1;
		
		score = (DF * numOfAppearances + AP_TITLE * title + PERSON_ORG * person_org + ENTITY *entity +AP_DYSCO * numOfAppearancesInDysco)/(DF + AP_TITLE + PERSON_ORG + ENTITY + AP_DYSCO);
		
		return score;
	}
	
	 @Override
	 public String toJSONString() {
         Gson gson = new GsonBuilder()
                .excludeFieldsWithoutExposeAnnotation()
                .create();
         return gson.toJson(this);
	 }
}
