/* $Id: NodeConsumer.java,v 1.1 2003/12/24 01:49:01 vpapad Exp $ */
package niagara.physical;

import niagara.utils.ShutdownException;

import org.w3c.dom.Node;

public interface NodeConsumer {
    void consume(Node n) throws ShutdownException, InterruptedException;
}
