package gr.iti.mklab.sfc;


import gr.iti.mklab.sfc.streams.StreamException;
import gr.iti.mklab.sfc.streams.StreamsManagerConfiguration;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.SAXException;

public class StreamsManagerApp {

	
	public static void main(String[] args) {
	
		Logger logger = LogManager.getLogger(StreamsManager.class);
		
		File streamConfigFile;
		if(args.length != 1 ) {
			streamConfigFile = new File("./conf/streams.conf.xml");
		}
		else {
			streamConfigFile = new File(args[0]);
		}
		
		StreamsManager manager = null;
		try {
			StreamsManagerConfiguration config = StreamsManagerConfiguration.readFromFile(streamConfigFile);		
	        
			manager = new StreamsManager(config);
			manager.open();
			
			Runtime.getRuntime().addShutdownHook(new Shutdown(manager));
			
			manager.start();
			
		} catch (ParserConfigurationException e) {
			logger.error(e);
		} catch (SAXException e) {
			logger.error(e);
		} catch (IOException e) {
			logger.error(e);
		} catch (StreamException e) {
			logger.error(e);
		} catch (Exception e) {
			logger.error(e);
		}	
		
		if(manager == null) {
			logger.error("");
			System.exit(-1);
		}
		
		logger.info("Stream manager initialized!");
		while(manager.isOpen()) {
			
			if(!manager.isRunning()) {
				logger.error("Stream Manager main thread is not running. Restart it.");
				manager.start();
			}
			
			manager.recheck();
			
			try {
				Thread.sleep(300000);
			} catch (InterruptedException e) {
				logger.error(e);
			}
		}
	}

}
