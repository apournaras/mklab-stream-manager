

package gr.iti.mklab.sfc.streams;

/**
 * An error that can occur during stream related operations
 *
 */
public class StreamError {

	private Exception ex;
	private String message;
	
	public StreamError(String message) {
		this.message = message;
	}

	public StreamError(String message, Exception ex) {
		this.ex = ex;
		this.message = message;
	}

	public Exception getException() {
		return ex;
	}

	public String getMessage() {
		return message;
	}
	
}
