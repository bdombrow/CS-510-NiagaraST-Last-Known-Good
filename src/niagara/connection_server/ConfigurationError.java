/* $Id: ConfigurationError.java,v 1.2 2003/09/23 23:10:25 vpapad Exp $ */
package niagara.connection_server;

public class ConfigurationError extends RuntimeException {
    /**
     * Constructor for ConfigurationError.
     * @param message
     */
    public ConfigurationError(String message) {
        super("Configuration error: " + message);
    }
}
