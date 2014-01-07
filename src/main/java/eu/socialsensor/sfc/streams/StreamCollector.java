package eu.socialsensor.sfc.streams;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;


import eu.socialsensor.framework.streams.StreamException;
import eu.socialsensor.sfc.builder.InputConfiguration;
import eu.socialsensor.sfc.streams.management.StreamsManager;

/**
 * Main class for the execution of StreamManager
 * defining the appropriate configuration file.
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 *
 */
public class StreamCollector {

	public static void main(String[] args) {
	
		Logger logger = Logger.getLogger(StreamCollector.class);
		
		File streamConfigFile;
		File inputConfigFile;
		
		if(args.length != 2 ) {
			streamConfigFile = new File("./conf/streams.conf.xml");
			inputConfigFile = new File("./conf/input.conf.xml");
		}
		else {
			streamConfigFile = new File(args[0]);
			inputConfigFile = new File(args[1]);
		}
		
		try {
			StreamsManagerConfiguration config = StreamsManagerConfiguration.readFromFile(streamConfigFile);		
			InputConfiguration input_config = InputConfiguration.readFromFile(inputConfigFile);		
	        
			StreamsManager manager = new StreamsManager(config,input_config);
			manager.open();
			
			waiting();
			
			//close manager
			logger.info("Close Stream manager.");
			manager.close();
			
		} catch (ParserConfigurationException e) {
			logger.error(e.getMessage());
		} catch (SAXException e) {
			logger.error(e.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
		} catch (StreamException e) {
			logger.error(e.getMessage());
		}
		
	}
	
	private static void waiting() {
		try {
			InetAddress inet = InetAddress.getByName(null);
			ServerSocket shutdownSocket = new ServerSocket(11111, 0, inet);
			shutdownSocket.accept();
			
			shutdownSocket.close();
		} catch (IOException e1) {
			System.out.println("Problems connecting to shutdown port");
		}
	}
}
