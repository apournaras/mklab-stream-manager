package eu.socialsensor.sfc.streams.filters;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.Configuration;
import eu.socialsensor.framework.client.dao.SourceDAO;
import eu.socialsensor.framework.client.dao.impl.SourceDAOImpl;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.Source;

public class ListItemFilter extends ItemFilter {

	private Map<String, Set<String>> usersToLists = new HashMap<String, Set<String>>();
	
	public ListItemFilter(Configuration configuration) {
		super(configuration);
		
		String host = configuration.getParameter("host");
		String database = configuration.getParameter("database");
		String collection = configuration.getParameter("collection");
		
		SourceDAO sourceDao = new SourceDAOImpl(host, database, collection);
		
		List<Source> sources = sourceDao.findAllSources();
		for(Source source : sources) {
			String user = source.getNetwork()+"#"+source.getId();
			
			//extract list
			String list = source.getList();
			if(list != null) {
				Set<String> lists = usersToLists.get(user);
				if(lists == null) {
					lists = new HashSet<String>();
				}
				lists.add(list);
				usersToLists.put(user, lists);
			}
		}
	}

	@Override
	public boolean accept(Item item) {
		if(usersToLists != null && getUserList(item) != null) {
			item.setList(getUserList(item));
			incrementAccepted();
			return true;
		}
		incrementDiscarded();
		return false;
	}

	@Override
	public String name() {
		return "ListItemFilter";
	}
	
	/**
	 * Returns the lists that the user associated with a given 
	 * item belongs to
	 * @param item
	 * @return
	 */
	private String[] getUserList(Item item) {
		
		Set<String> lists = new HashSet<String>();
		if(usersToLists == null) {
			Logger.getLogger(ListItemFilter.class).info("User list is null");
			return null;
		}
			
		if(item.getUserId() == null) {
			Logger.getLogger(ListItemFilter.class).info("User in item is null");
			return null;
		}
				
		Set<String> userLists = usersToLists.get(item.getUserId());
		if(userLists != null) {
			lists.addAll(userLists);
		}
		
		String[] mentions = item.getMentions();
		if(mentions != null) {
			for(String mention : mentions) {
				userLists = usersToLists.get(mention);
				if(userLists != null) {
					lists.addAll(userLists);
				}
			}
		}
		
		String refUserId = item.getReferencedUserId();
		if(refUserId != null) {
			userLists = usersToLists.get(refUserId);
			if(userLists != null) {
				lists.addAll(userLists);
			}
		}
		
		if(lists.size() > 0) {
			return lists.toArray(new String[lists.size()]);
		}
		else {
			return null;
		}
		
	}
	
}
