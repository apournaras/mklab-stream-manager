package gr.iti.mklab.sfc.processors;

import java.util.Arrays;
import java.util.List;

import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.Location;
import gr.iti.mklab.framework.common.domain.StreamUser;
import gr.iti.mklab.framework.common.domain.config.Configuration;

public class LocationEstimation extends Processor {

	private List<String> codesList;
	private List<String> countriesList;


	public LocationEstimation(Configuration configuration) {
		super(configuration);
		
		codesList = Arrays.asList(codes);
		countriesList = Arrays.asList(countries);
	}

	@Override
	public void process(Item item) {
		Location location = item.getLocation();
		if(location == null) {
			StreamUser user = item.getStreamUser();
			if(user != null) {
				String timezone = user.getTimezone();
				if(timezone != null && codesList.contains(timezone)) {
					int index = codesList.indexOf(timezone);
					String country = countriesList.get(index);
				
					location = new Location();
					location.setCountryName(country);
					
					item.setLocation(location);
				}
			}
		}
	}

	private static String[] countries = {
			"Abu Dhabi","Adelaide","Alaska","Almaty","Amsterdam","Arizona","Astana","Athens","Atlantic Time (Canada)",
			"Auckland","Azores","Baghdad","Baku","Bangkok","Beijing","Belgrade","Berlin","Bern","Bogota","Brasilia",
			"Bratislava","Brisbane","Brussels","Bucharest","Budapest","Buenos Aires","Cairo","Canberra","Cape Verde Is.",
			"Caracas","Casablanca","Central America","Central Time (US & Canada)","Chennai","Chihuahua","Chongqing",
			"Copenhagen","Darwin","Dhaka","Dublin","Eastern Time (US & Canada)","Edinburgh","Ekaterinburg","Fiji",
			"Georgetown","Greenland","Guadalajara","Guam","Hanoi","Harare","Hawaii","Helsinki","Hobart",
			"Hong Kong","Indiana (East)","International Date Line West","Irkutsk","Islamabad","Istanbul",
			"Jakarta","Jerusalem","Kabul","Kamchatka","Karachi","Kathmandu","Kolkata","Krasnoyarsk","Kuala Lumpur",
			"Kuwait","Kyiv","La Paz","Lima","Lisbon","Ljubljana","London","Madrid","Magadan","Marshall Is.",
			"Mazatlan","Melbourne","Mexico City","Mid-Atlantic","Midway Island","Minsk","Monrovia",
			"Monterrey","Moscow","Mountain Time (US & Canada)","Mumbai","Muscat","Nairobi","New Caledonia","New Delhi",
			"Newfoundland","Novosibirsk","Nuku'alofa","Osaka","Pacific Time (US & Canada)","Paris,Perth","Port Moresby",
			"Prague","Pretoria","Quito","Rangoon","Riga","Riyadh","Rome","Samoa","Santiago","Sapporo","Sarajevo",
			"Saskatchewan","Seoul","Singapore","Skopje","Sofia","Solomon Is.","Sri Jayawardenepura","St. Petersburg",
			"Stockholm","Sydney","Taipei","Tallinn","Tashkent","Tbilisi","Tehran","Tijuana","Tokyo","Ulaan Bataar","Urumqi",
			"Vienna","Vilnius","Vladivostok","Volgograd","Warsaw","Wellington","West Central Africa","Yakutsk",
			"Yerevan","Zagreb"};   


	private static String[] codes = {
			"United Arab Emirates","Australia","United States","Kazakhstan","Netherlands","United States","Kazakhstan","Greece","Canada",
			"New Zealand","Portugal","Iraq","Azerbaijan","Thailand","China","Serbia","Germany","Switzerland","Colombia","Brazil",
			"Slovakia","Australia","Belgium","Romania","Hungary","Argetina","Egypt","Australia","Cape Verde",
			"Venezuela","Morocco","United States","United States","India","Mexico","China",
			"Denmark","Australia","Bangladesh","Ireland","United States","United Kingdom","Russia","Fiji",
			"United States","Greenland","Mexico","United States","Vietnam","Zimbabwe","United States","Finland","Australia",
			"China","United States","United States","Russia","Pakistan","Turkey",
			"Indonesia","Israel","Afghanistan","Russia","Pakistan","Nepal","United States","Russia","Malaysia",
			"Kuwait","Russia","Volivia","Peru","Portugal","Slovenia","United Kingdom","Spain","Russia","United States",
			"Mexico","Australia","Mexico","United States","United States","Belarus","Liberia",
			"Mexico","Russia","United States","India","Oman","Kenya","France","India",
			"Canada","Russia","United States","Japan","United States","France","Papua New Guinea",
			"Czech","South Africa","Ecuador","Burma","Latvia","Saudi Arabia","Italy","New Zealand","Chile","Japan","Bosnia and Herzegovina",
			"Canada"," South Korea","Malaysia","Macedonia","Bulgary","United States","Sri Lanka","Russia",
			"Sweden ","Australia","China","Estonia","Uzbekistan","Georgia","Iran","Mexico","Japan","Mongolia","China",
			"Austria","Lithuania","Russia","Russia","Poland","New Zealand","United States","Russia",
			"Armenia","Croatia"};
}
