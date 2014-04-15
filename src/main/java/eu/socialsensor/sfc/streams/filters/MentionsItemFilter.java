package eu.socialsensor.sfc.streams.filters;

import java.util.List;

import eu.socialsensor.framework.common.domain.Item;

public class MentionsItemFilter implements ItemFilter {

	private List<String> ids;
	private String listId;

	public MentionsItemFilter(String listId, List<String> ids) {
		this.ids  = ids;
		this.listId = listId;
	}
	
	@Override
	public boolean accept(Item item) {
		String[] lists = item.getList();
		if(lists == null || lists.length==0 || lists.length>1)
			return true;
		
		if(!lists[0].equals(listId))
			return true;
		
		String uid = item.getUserId();
		if (!ids.contains(uid))
			return false;
		
		return true;
	}

}