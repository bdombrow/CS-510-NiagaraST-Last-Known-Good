/* $Id: ConfigurationError.java,v 1.1 2002/10/23 22:33:59 vpapad Exp $ */
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
