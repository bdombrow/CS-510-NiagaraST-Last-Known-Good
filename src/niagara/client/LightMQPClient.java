/* $Id$ */
package niagara.client;

/** LightMQPClient is used by the server to route queries to other servers*/
public class LightMQPClient extends MQPClient {
    private String errorMessage;
    private boolean queryDone;
    
    public LightMQPClient(String host, int port) {
        super(host, port);
    }
    
    public synchronized void sendQuery(String query, int timeout) {
        errorMessage = null;
        queryDone = false;
        try {                        
            cm.executeQuery(new MQPQuery(query),
                            Integer.MAX_VALUE);
            wait(timeout);
        } catch (ClientException ce) {
            errorMessage = ce.getMessage();
        } catch (InterruptedException ie) {
            errorMessage = " interrupted";
        }
        
        if (!queryDone) {
            errorMessage = " timed out";
        }
    }
            
    /**
     * @see niagara.client.UIDriverIF#errorMessage(String)
     */
    public synchronized void errorMessage(String err) {
        errorMessage = err;
        queryDone = true;
        notify();
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    /**
     * @see niagara.client.UIDriverIF#notifyFinalResult(int)
     */
    public synchronized void notifyFinalResult(int id) {
        queryDone = true;
        notify();
    }

    /**
     * @see niagara.client.UIDriverIF#notifyNew(int)
     */
    public void notifyNew(int id) {
        ;
    }
}
