/* $Id: InvalidPlanException.java,v 1.1 2003/02/25 06:16:28 vpapad Exp $ */
package niagara.connection_server;

public class InvalidPlanException extends Exception {
    public InvalidPlanException() {
        super("Invalid Plan Exception: ");
    }
    public InvalidPlanException(String msg) {
        super("Invalid Plan Exception: " + msg + " ");
    }
}