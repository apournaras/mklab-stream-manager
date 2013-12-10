package eu.socialsensor.sfc.streams.store;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import eu.socialsensor.framework.client.lucene.TweetAnalyzer;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.sfc.streams.StorageConfiguration;

/**
 * Class for storing items to lucene storage
 * @author manosetro
 * @email  manosetro@iti.gr
 *
 */
public class LuceneStorage implements StreamUpdateStorage {

	private static IndexWriter writer;
	private static IndexReader reader;
	private static IndexSearcher searcher;
	private static FSDirectory dir;
	  
	private static final String FIELD_ID = "id";
	private static final String FIELD_TEXT = "text";
	private static final String FIELD_TIME = "time";
	
	private static final String NGRAMS= "storage.lucene.ngrams";
	private static final String STOPWORDS = "storage.lucene.stopwords";
	private static final String INDEX_FILE = "storage.lucene.indexFile";
	
	
	private static List<String> tweetIds;
	private FieldType fieldTypeText;
	private FieldType fieldTypeTimeslotId;
		
	private Logger  logger = Logger.getLogger(LuceneStorage.class);
	
	
	public LuceneStorage(StorageConfiguration config) throws IOException {
		logger.info("Create a LuceneStorage instance");
		
		
		//Timer timer = new Timer(); 
		//timer.schedule(new Committer(), (long)30000, (long)30000);
		
		String ngrams_str = config.getParameter(LuceneStorage.NGRAMS);
		String stopwords_file = config.getParameter(LuceneStorage.STOPWORDS);
		String indexFile = config.getParameter(LuceneStorage.INDEX_FILE);
		
		tweetIds = new ArrayList<String>();
		
		dir = FSDirectory.open(new File(indexFile));
		int ngrams = ngrams_str==null ? TweetAnalyzer.DEFAULT_NGRAMS : Integer.parseInt(ngrams_str);
		
		TweetAnalyzer tweetAnalyzer;
		File f;
		if( (stopwords_file != null) && ((f = new File(stopwords_file)).exists())) {
			@SuppressWarnings("unchecked")
			List<String> stopwordsLines = FileUtils.readLines(f, "utf-8");
			Set<String> stopwordsSet = new HashSet<String>(stopwordsLines);
			tweetAnalyzer = new TweetAnalyzer(Version.LUCENE_40, stopwordsSet, ngrams);
		}
		else {
			tweetAnalyzer= new TweetAnalyzer(Version.LUCENE_40);
		}
		
		IndexWriterConfig index_config = new IndexWriterConfig(Version.LUCENE_40, tweetAnalyzer);
		writer = new IndexWriter(dir, index_config);
		
		fieldTypeText = new FieldType();
		fieldTypeText.setIndexed(true);
		fieldTypeText.setStored(true);
		fieldTypeText.setTokenized(true);
		fieldTypeText.setStoreTermVectors(true);
		fieldTypeText.setStoreTermVectorPositions(true);
		fieldTypeText.freeze();
		
		fieldTypeTimeslotId = new FieldType();
		fieldTypeTimeslotId.setIndexed(true);
		fieldTypeTimeslotId.setStored(true);
		fieldTypeTimeslotId.setTokenized(false);
		fieldTypeTimeslotId.freeze();
	}
	
	@Override
	public void store(Item item) throws IOException {
		//logger.info("Store item with id: "+item.getId()+" in the Lucene index");
		Document doc = new Document();
		doc.add(new StoredField(FIELD_ID, item.getId()));
		doc.add(new Field(FIELD_TEXT, item.getTitle(), fieldTypeText));
		long date = item.getPublicationTime();
		doc.add(new LongField(FIELD_TIME, date, Field.Store.NO));
		//doc.add(new Field(FIELD_TIMESLOT_ID, item.getTimeslotId(), fieldTypeTimeslotId));
		
		try {
			if (writer == null || exists(item.getId()) || tweetIds.contains(item.getId()))
				return;
			
			long t = System.currentTimeMillis();
			writer.addDocument(doc);
			tweetIds.add(item.getId());
			t = System.currentTimeMillis() - t;
			logger.info("Store item " + item.getId() + " in Lucene took " + t +" msecs");
		} catch (AlreadyClosedException e) {
			System.err.println("Lucene index is closed");
		}
		
	}

	public boolean exists(Object id) {
		
		try {
			reader = DirectoryReader.open(dir);
			searcher = new IndexSearcher(reader);
			Query query = new TermQuery(new Term(FIELD_ID, id.toString()));
			
			TopDocs topDocs = searcher.search(query, 1);
			if(topDocs.totalHits>0) {
				return true;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return true;
		}
		return false;
	}
	
	public String get(Object id) {
		logger.info("Get item with id: "+id+" from the Lucene index");
		String result=null;
		try {
			reader = DirectoryReader.open(dir);
			searcher = new IndexSearcher(reader);
			Query query = new TermQuery(new Term(FIELD_ID, id.toString()));
			
			TopDocs topDocs = searcher.search(query, 1);
		
			if (topDocs.totalHits!=0)
			{	
				ScoreDoc[] hits = topDocs.scoreDocs;
				result=reader.document(hits[0].doc).get(FIELD_TEXT);
			}
		
			reader.close();
			return result;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public boolean delete(String id) {
		//logger.info("Delete item with id: "+ id+" from the Lucene index");
		
		try {
			Query query = new TermQuery(new Term(FIELD_ID, id.toString()));
			writer.deleteDocuments(query);
			tweetIds.remove(id);
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	private void commit() throws CorruptIndexException, IOException {
		if (writer == null)
			return;
		
		long t = System.currentTimeMillis();
		writer.commit();
		tweetIds.clear();
		t = System.currentTimeMillis() - t;
		logger.info("Lucene commit took " + t +" msecs");
	}

	@Override
	public void open() throws IOException {
		writer.commit();
		tweetIds.clear();
	}

	@Override
	public void close() {
		if (writer!=null)
			try {
				writer.close();
				tweetIds.clear();
			} catch (CorruptIndexException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		if (dir!=null) dir.close();
	}
	
	@Override
	public void updateTimeslot() {
		try {
			commit();
		} catch (Exception e) {
			logger.error(e);
		}
	}

	@Override
	public void update(Item update) throws IOException {
		
	}
	
	
	
//	private class Committer extends TimerTask {
//		@Override
//		public void run() {
//			try {
//				commit();
//			} catch (CorruptIndexException e) {
//				e.printStackTrace();
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}	
//	}
	
}