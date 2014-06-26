package eu.socialsensor.sfc.streams.filters;

import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.sfc.streams.FilterConfiguration;

public class TokensFilter  extends ItemFilter {

	private int maxTokens;

	public TokensFilter(FilterConfiguration configuration) {
		super(configuration);
		String lenStr =configuration.getParameter("maxTokens", "6");
		this.maxTokens  = Integer.parseInt(lenStr);
		
		Logger.getLogger(TokensFilter.class).info("Initialized. Max Number of Tokemns: " + maxTokens);
	}

	@Override
	public boolean accept(Item item) {
		String title = item.getTitle();
		if(title == null)
			return false;
		
		try {
			Reader reader = new StringReader(title);
			TokenStream tokenizer = new StandardTokenizer(Version.LUCENE_40, reader);
			
			List<String> tokens = new ArrayList<String>();
			CharTermAttribute charTermAtt = tokenizer.addAttribute(CharTermAttribute.class);
			tokenizer.reset();
			while (tokenizer.incrementToken()) {
				String token = charTermAtt.toString();
				if(token.contains("http") || token.contains("."))
					continue;
					
				tokens.add(token);
			}
			tokenizer.end();  
			tokenizer.close();

			if(tokens.size() < maxTokens) {
				return false;
			}
			
		} catch (Exception e) {
			Logger.getLogger(TokensFilter.class).error(e);
			return false;
		}
		return true;
	}
}
