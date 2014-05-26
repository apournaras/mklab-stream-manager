package eu.socialsensor.sfc.streams.store;

import java.io.IOException;

import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.MediaItem;
import eu.socialsensor.sfc.streams.StorageConfiguration;
import gr.iti.mklab.download.ImageDownload;
import gr.iti.mklab.download.ImageDownloadResult;

public class ImageDownloaderStorage implements StreamUpdateStorage{
	private static String FILE_NAME = "filename";
	
	private String destinationFile = null;
	
	private String storageName = "ImageDownloader";
	
	public ImageDownloaderStorage(StorageConfiguration config){
		this.destinationFile = config.getParameter(ImageDownloaderStorage.FILE_NAME);
	}
	
	public ImageDownloaderStorage(String destinationFile){
		this.destinationFile = destinationFile;
	}
	
	public boolean open(){
		if(destinationFile != null)
			return true;
		
		return false;
	}

	public void store(Item update) throws IOException{
		
		for(MediaItem mItem : update.getMediaItems()){
			
			String urlStr = mItem.getUrl();
			String id = mItem.getId();
			
			boolean saveThumb = false;
			boolean saveOriginal = true;
			boolean followRedirects = false;
			
			ImageDownload imdown = new ImageDownload(urlStr, id, destinationFile, saveThumb, saveOriginal,
					followRedirects);
			imdown.setDebug(true);

			ImageDownloadResult imdr;
			
			try {
				imdr = imdown.call();
				//System.out.println("Getting the BufferedImage object of the downloaded image.");
				imdr.getImage();
				//System.out.println("Reading the downloaded image thumbnail into a BufferedImage object.");
				//ImageIO.read(new File(destinationFile + id + "-thumb.jpg"));
				//System.out.println("Success!");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		
	}
	
	public void update(Item update) throws IOException{
		
	}
	
	
	public boolean delete(String id) throws IOException{
		return false;
	}
	
	public boolean deleteItemsOlderThan(long dateThreshold) throws IOException{
		return false;
	}
	
	public boolean checkStatus(StreamUpdateStorage storage){
		return false;
	}
	
	public void updateTimeslot(){
		
	}
	
	/**
	 * Close the store
	 */
	public void close(){
		
	}
	
	public String getStorageName(){
		return this.storageName;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
