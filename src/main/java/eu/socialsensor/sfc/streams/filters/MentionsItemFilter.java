package eu.socialsensor.sfc.streams.filters;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.Configuration;
import eu.socialsensor.framework.client.dao.SourceDAO;
import eu.socialsensor.framework.client.dao.impl.SourceDAOImpl;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.Source;

public class MentionsItemFilter extends ItemFilter {

	private List<String> ids;
	private String listId;

	public MentionsItemFilter(Configuration configuration) {
		super(configuration);
		try {
		this.listId =configuration.getParameter("listId");
		
		String host =configuration.getParameter("host");
		String database =configuration.getParameter("database");
		String collection =configuration.getParameter("collection");
			
		SourceDAO dao = new SourceDAOImpl(host, database, collection);
		List<Source> sources = dao.findListSources(listId);
		ids = new ArrayList<String>();
		for(Source source : sources) {
			ids.add(source.getNetwork() + "#" + source.getId());
		}
		
		Logger.getLogger(MentionsItemFilter.class).info("Initialized. " + 
				ids.size() + " ids from list " + listId + " to be used in mentions filter");
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public boolean accept(Item item) {
		
		String[] mentions = item.getMentions();
		if(mentions != null && mentions.length > 2) {
			incrementDiscarded();
			return false;
		}
		
		String[] lists = item.getList();
		if(lists == null || lists.length==0 || lists.length>1) {
			incrementAccepted();
			return true;
		}
		
		if(!lists[0].equals(listId)) {
			incrementAccepted();
			return true;
		}
		
		String uid = item.getUserId();
		if (!ids.contains(uid)) {
			incrementDiscarded();
			return false;
		}
		
		return true;
	}

	@Override
	public String name() {
		return "MentionsItemFilter";
	}
	
}