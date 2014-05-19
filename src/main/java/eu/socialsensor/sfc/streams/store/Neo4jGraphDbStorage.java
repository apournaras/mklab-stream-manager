package eu.socialsensor.sfc.streams.store;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import org.neo4j.jdbc.Driver;
import org.neo4j.jdbc.Neo4jConnection;

import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.sfc.streams.StorageConfiguration;

public class Neo4jGraphDbStorage implements StreamUpdateStorage {
	
	private static String HOST = "neo4j.host";
	
	private static String NODE_USER_LABEL = "neo4j.node.user.label";
	private static String NODE_USER_ID = "neo4j.node.userid";
	
	private static String RELATIONSHIP_RETWEETS = "neo4j.relationship.retweets";
	private static String RELATIONSHIP_MENTIONS = "neo4j.relationship.mentions";
	private static String RELATIONSHIP_PROPERTY_TIMESTAMP = "neo4j.relationship.property.timestamp";
	private static String RELATIONSHIP_PROPERTY_TWEETID = "neo4j.relationship.property.tweetid";
	
	private String storageName = "Neo4j";
	
	private String host;
	
	private String nodeUserLabel;
	private String nodeUserId;
	
	private String relationshipReTweets;
	private String relationshipMentions;
	
	private String relationshipPropertyTimestamp;
	private String relationshipPropertyTweedId;
	
	Neo4jConnection connection;
	
	public Neo4jGraphDbStorage(StorageConfiguration config) {
		
		this.host = config.getParameter(Neo4jGraphDbStorage.HOST);
		
		this.nodeUserLabel = config.getParameter(Neo4jGraphDbStorage.NODE_USER_LABEL);
		this.nodeUserId = config.getParameter(Neo4jGraphDbStorage.NODE_USER_ID);
		
		this.relationshipReTweets = config.getParameter(Neo4jGraphDbStorage.RELATIONSHIP_RETWEETS);
		this.relationshipMentions = config.getParameter(Neo4jGraphDbStorage.RELATIONSHIP_MENTIONS);
		
		this.relationshipPropertyTimestamp = config.getParameter(Neo4jGraphDbStorage.RELATIONSHIP_PROPERTY_TIMESTAMP);
		this.relationshipPropertyTweedId = config.getParameter(Neo4jGraphDbStorage.RELATIONSHIP_PROPERTY_TWEETID);
	}

	@Override
	public boolean open() {
		try {
			Class.forName("org.neo4j.jdbc.Driver");
			connection = new Driver().connect(host, new Properties());
			createIndex(nodeUserLabel, nodeUserId);
		} 
		catch (ClassNotFoundException e) {
			e.printStackTrace();
		} 
		catch (SQLException e) {
			e.printStackTrace();
		}
		
		return true;
	}

	@Override
	public void store(Item item) throws IOException {
		String userId = item.getUserId().split("#")[1];
		String tweetId = item.getId().split("#")[1];
		long timestamp = item.getPublicationTime();
//		String title = item.getTitle();
		
		try {
			getOrCreateVertex(userId, nodeUserId);
			//handle mentions
			String[] mentions = item.getMentions();
			for(String userMention : mentions) {
				
				//handle retweets
				if(!item.isOriginal()) {
					String userRetweets = item.getReferencedUserId().split("#")[1];
					getOrCreateVertex(userRetweets, nodeUserId);
					getOrCreateEdge(userId, userRetweets, relationshipReTweets);
					addEdgeProperty(userId, userRetweets, relationshipPropertyTweedId, tweetId);
					addEdgeProperty(userId, userRetweets, relationshipPropertyTimestamp, String.valueOf(timestamp));
				}
				else {
					userMention = userMention.split("#")[1];
					getOrCreateVertex(userMention, nodeUserId);
					getOrCreateEdge(userId, userMention, relationshipMentions);
					addEdgeProperty(userId, userMention, relationshipPropertyTweedId, tweetId);
					addEdgeProperty(userId, userMention, relationshipPropertyTimestamp, String.valueOf(timestamp));
				}
				
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void update(Item update) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean delete(String id) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteItemsOlderThan(long dateThreshold) throws IOException {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean checkStatus(StreamUpdateStorage storage) {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public void updateTimeslot() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		try {
			connection.close();
		} 
		catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public String getStorageName() {
		return storageName;
	}
	
	private void createIndex(String label, String property) throws SQLException {
		String statement = "CREATE INDEX ON :" + nodeUserLabel 
				+ "(" + nodeUserId + ")";
		connection.createStatement().executeQuery(statement);
	}
	
	private void getOrCreateVertex(String nodeId, String nodeKey) throws SQLException {
		String mergeVertex = 
				"MERGE (n:" + nodeUserLabel + " {" + nodeKey + ":" + nodeId + "}) ";
		connection.createStatement().executeQuery(mergeVertex);
	}
	
	private void getOrCreateEdge(String source, String destination, String label) throws SQLException {
		String mergeEdge = 
				"MATCH (n1:" + nodeUserLabel + " {" + nodeUserId + ":" + source + "})" +
				"MATCH (n2:" + nodeUserLabel + " {" + nodeUserId + ":" + destination + "})" +
				"CREATE (n1)-[:" + label + "]->(n2)";
		connection.createStatement().executeQuery(mergeEdge);
	}
	
	private void addEdgeProperty(String source, String destination, String propertyName, 
			String propertyValue) throws SQLException {
		String addEdgeProperty = 
				"MATCH (n1:" + nodeUserLabel + " {" + nodeUserId + ":" + source + "})" +
				"MATCH (n2:" + nodeUserLabel + " {" + nodeUserId + ":" + destination + "})" +
				"MATCH (n1)-[r]->(n2)" +
				"SET r." + propertyName + " = " + propertyValue;
		connection.createStatement().executeQuery(addEdgeProperty);
	}
	
}
