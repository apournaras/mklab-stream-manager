package eu.socialsensor.sfc.streams;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
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
		
		if(new File("log4j.properties").exists()) {
			PropertyConfigurator.configure("log4j.properties");
		}
		
		Logger logger = Logger.getLogger(StreamCollector.class);
		
		File streamConfigFile, inputConfigFile;
		if(args.length != 2 ) {
			streamConfigFile = new File("./conf/streams.conf.xml");
			inputConfigFile = new File("./conf/input.conf.xml");
		}
		else {
			streamConfigFile = new File(args[0]);
			inputConfigFile = new File(args[1]);
		}
		
		StreamsManager manager = null;
		try {
			StreamsManagerConfiguration config = StreamsManagerConfiguration.readFromFile(streamConfigFile);		
			InputConfiguration input_config = InputConfiguration.readFromFile(inputConfigFile);		
	        
			manager = new StreamsManager(config, input_config);
			manager.open();
			
			Runtime.getRuntime().addShutdownHook(new Shutdown(manager));
			
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
	}
	
	public static void waiting() throws Exception {
		try {
			InetAddress inet = InetAddress.getByName(null);
			ServerSocket shutdownSocket = new ServerSocket(11111, 0, inet);
			shutdownSocket.accept();
			
			shutdownSocket.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new Exception(e);
		} 
	}
	
	
	/**
	 * Class in case system is shutdown. 
	 * Responsible to close all services that are running at the time being
	 * @author ailiakop
	 *
	 */
	private static class Shutdown extends Thread {
		private StreamsManager _manager = null;
		private Logger _logger = Logger.getLogger(Shutdown.class);
		
		public Shutdown(StreamsManager manager) {
			this._manager = manager;
		}

		public void run() {
			_logger.info("Shutting down stream manager...");
			if (_manager != null) {
				try {
					_manager.close();
				} catch (StreamException e) {
					_logger.error(e);
					e.printStackTrace();
				}
			}
			_logger.info("Done...");
		}
	}
	
}
