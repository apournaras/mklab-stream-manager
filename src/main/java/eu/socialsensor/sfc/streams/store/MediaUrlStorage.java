package eu.socialsensor.sfc.streams.store;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.tika.io.IOUtils;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.sfc.streams.StorageConfiguration;

/**
 * Class for storing mediaUrls to a file
 * @author manosetro
 * @email  manosetro@iti.gr
 *
 */
public class MediaUrlStorage implements StreamUpdateStorage {

	private static Pattern instagramPattern = Pattern.compile("http://instagram.com/p/(.+)/");
	private static Pattern youtubePattern = Pattern.compile("http://www.youtube.com/watch?.*v=(.+)(&.+=.+)+");
	private static Pattern vimeoPattern = Pattern.compile("http://vimeo.com/([0-9]+)/*$");
	private static Pattern twitpicPattern = Pattern.compile("http://twitpic.com/([A-Za-z0-9]+)/*.*$");
	private static Pattern dailymotionPattern = Pattern.compile("http://www.dailymotion.com/video/([A-Za-z0-9]+)_.*$");
	
	
	public static final String STORE_FILE = "./media_urls.txt";
	
	private PrintWriter out = null;
	private JsonParser parser = new JsonParser();
	
	
	private static String instagramRegex = "http://instagram.com/p/*";
	 Pattern pattern = Pattern.compile(instagramRegex);

	
	public MediaUrlStorage(StorageConfiguration config) throws IOException {
		
	}
	
	public static void main(String[] args) {
	
		String url1 = "http://instagram.com/p/12323211/";
		String url2 = "http://www.youtube.com/watch?v=jHdTve2PpeI&alt=json";
		String url3 = "http://vimeo.com/22697740/";
		String url4 = "http://twitpic.com/d255om/";
		String url5 = "http://www.dailymotion.com/video/xy4ybs_pregnant-kim-kardashian-flaunts-baby-bump-and-booty-at-gas-station_people#.UeAW1OeKYdE";
		
				Matcher instagramMatcher = instagramPattern.matcher(url1);
		Matcher youtubeMatcher = youtubePattern.matcher(url2);
		Matcher vimeoMatcher = vimeoPattern.matcher(url3);
		Matcher twitpicMatcher = twitpicPattern.matcher(url4);
		Matcher dailymotionMatcher = dailymotionPattern.matcher(url5);
		
		if(instagramMatcher.matches()) {
			System.out.println("Instagram: " + instagramMatcher.group(1));
		}
		if(youtubeMatcher.matches()) {
			System.out.println("Youtube: " + youtubeMatcher.group(1));
		}
		if(vimeoMatcher.matches()) {
			System.out.println("Vimeo: " + vimeoMatcher.group(1));
		}
		if(twitpicMatcher.matches()){
			System.out.println("Twitpic: " + twitpicMatcher.group(1));
		}
		if(dailymotionMatcher.matches()){
			System.out.println("Dailymotion: " + dailymotionMatcher.group(1));
		}
	}
	
	@Override
	public void open() throws IOException {
		File storeFile = new File(STORE_FILE);
		out = new PrintWriter(new BufferedWriter(new FileWriter(storeFile, false)));
	}

	@Override
	public void store(Item item) throws IOException {
		
		URL[] links = item.getLinks();
		if(links != null && links.length>0) {
			for(URL link : links) {
				String s = getLinkSource(link);
				if(s != null) {
					String str = test(s);
					if(str!=null) {
						out.write(s +" => " + str + " \n");
						out.flush();
					}
				}
			}
		}
		
	}

	private String test(String url) {
		Matcher instagramMatcher = instagramPattern.matcher(url);
		Matcher youtubeMatcher = youtubePattern.matcher(url);
		Matcher vimeoMatcher = vimeoPattern.matcher(url);
		Matcher twitpicMatcher = twitpicPattern.matcher(url);
		Matcher dailymotionMatcher = dailymotionPattern.matcher(url);
		
		if(instagramMatcher.matches()) {
			return "Instagram id: " + instagramMatcher.group(1);
		}
		else if(youtubeMatcher.matches()) {
			return  "Youtube id: " + youtubeMatcher.group(1);
		}
		else if(vimeoMatcher.matches()) {
			return "Vimeo id: " + vimeoMatcher.group(1);
		}
		else if(twitpicMatcher.matches()) {
			return "Twitpic id: " + twitpicMatcher.group(1);
		}
		else if(dailymotionMatcher.matches()){
			System.out.println("Dailymotion: " + dailymotionMatcher.group(1));
		}
		return null;
	}
	private String getLinkSource(URL link) {
		
		URL url;
		try {
			url = new URL("http://api.longurl.org/v2/expand?format=json&url=" 
					+ link.toString());
			
			InputStream stream = url.openStream();
			
			StringWriter output = new StringWriter();
			IOUtils.copy(stream, output);
			
			String response = output.toString();
			
			JsonElement jelement = parser.parse(response);
		    JsonObject  jobject = jelement.getAsJsonObject();
		    
		    String urlStr = jobject.get("long-url").getAsString();
		    
			return urlStr;
		} catch (IOException e) {
			return null;
		}
		
		
	}
	
	@Override
	public boolean delete(String id) throws IOException {
		return false;
	}

	@Override
	public void updateTimeslot() {
		
	}
	
	@Override
	public void close() {
		out.close();
	}

}
