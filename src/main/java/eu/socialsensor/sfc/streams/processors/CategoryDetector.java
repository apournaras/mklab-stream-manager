package eu.socialsensor.sfc.streams.processors;

import java.util.List;
import java.util.Map;

import eu.socialsensor.framework.Configuration;
import eu.socialsensor.framework.client.dao.ExpertDAO;
import eu.socialsensor.framework.client.dao.impl.ExpertDAOImpl;
import eu.socialsensor.framework.common.domain.Expert;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.StreamUser.Category;

public class CategoryDetector extends Processor {

	private Map<String, Category> usersToCategory = null;
	
	public CategoryDetector(Configuration configuration) {
		super(configuration);
		
		String host =configuration.getParameter("host");
		String database =configuration.getParameter("database");
		String collection =configuration.getParameter("collection");
		
		ExpertDAO expertsDao = new ExpertDAOImpl(host, database, collection);
		
		List<Expert> experts = expertsDao.getExperts();
		if(experts != null) {
			for(Expert expert : experts) {
				String user = "Twitter#"+expert.getId();
				usersToCategory.put(user, expert.getCategory());
			}
		}
		
	}

	@Override
	public void process(Item item) {
		if(usersToCategory != null && getUserCategory(item) != null) {
			item.setCategory(getUserCategory(item));
		}
	}

	/**
	 * Returns the category that a user associated with a given
	 * item belongs to
	 * @param item
	 * @return
	 */
	private Category getUserCategory(Item item) {
		if(usersToCategory == null){
			return null;
		}
		
		String userId = item.getUserId();
		
		if(userId == null){
			return null;
		}	
		return usersToCategory.get(userId);
	}
	
}
