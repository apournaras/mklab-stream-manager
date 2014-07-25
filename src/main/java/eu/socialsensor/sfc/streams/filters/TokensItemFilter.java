package eu.socialsensor.sfc.streams.filters;

import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.StreamUser;
import eu.socialsensor.sfc.streams.FilterConfiguration;

public class TokensItemFilter  extends ItemFilter {

	private int maxTokens;
	
	public TokensItemFilter(FilterConfiguration configuration) {
		super(configuration);
		String lenStr =configuration.getParameter("maxTokens", "6");
		this.maxTokens  = Integer.parseInt(lenStr);
		
		Logger.getLogger(TokensItemFilter.class).info("Initialized. Max Number of Tokemns: " + maxTokens);
	}

	@Override
	public boolean accept(Item item) {
		
		try {
			String title = item.getTitle();
			if(title == null) {
				incrementDiscarded();
				return false;
			}
			
			StreamUser streamUser = item.getStreamUser();
			if(streamUser == null || streamUser.isVerified()) {
				incrementAccepted();
				return true;
			}
			
			Reader reader = new StringReader(title);
			TokenStream tokenizer = new WhitespaceTokenizer(Version.LUCENE_40, reader);
			
			List<String> tokens = new ArrayList<String>();
			CharTermAttribute charTermAtt = tokenizer.addAttribute(CharTermAttribute.class);
			tokenizer.reset();
			while (tokenizer.incrementToken()) {
				String token = charTermAtt.toString();
				if(token.contains("http") || token.contains(".") || token.length() == 1)
					continue;
					
				tokens.add(token);
			}
			tokenizer.end();  
			tokenizer.close();

			if(tokens.size() < maxTokens) {
				incrementDiscarded();
				return false;
			}
			
		} catch (Exception e) {
			Logger.getLogger(TokensItemFilter.class).error(e);
			incrementDiscarded();
			return false;
		}
		
		incrementAccepted();
		return true;
	}

	@Override
	public String name() {
		return "TokensItemFilter";
	}
	
}
