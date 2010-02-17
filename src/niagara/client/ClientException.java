package niagara.client;

/**
 * ClientException is a generic exception used on the client
 *
 * @version 1.0
 *
 */

@SuppressWarnings("serial")
public class ClientException extends Exception {

   ClientException() {
	super("ClientException");
   }

   ClientException(String msg) {
	super("ClientException " + msg);
   }

}
