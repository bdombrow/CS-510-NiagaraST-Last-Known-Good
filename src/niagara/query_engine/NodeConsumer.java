/* $Id: NodeConsumer.java,v 1.1 2003/09/26 21:19:32 vpapad Exp $ */
package niagara.query_engine;

import niagara.utils.ShutdownException;

import org.w3c.dom.Node;

public interface NodeConsumer {
    void consume(Node n) throws ShutdownException, InterruptedException;
}
