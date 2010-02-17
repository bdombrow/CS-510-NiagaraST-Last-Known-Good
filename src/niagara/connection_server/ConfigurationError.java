package niagara.connection_server;

@SuppressWarnings("serial")
public class ConfigurationError extends RuntimeException {
	/**
	 * Constructor for ConfigurationError.
	 * 
	 * @param message
	 */
	public ConfigurationError(String message) {
		super("Configuration error: " + message);
	}
}
