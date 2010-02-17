package niagara.physical;

import niagara.utils.ShutdownException;

import org.w3c.dom.Node;

public interface NodeConsumer {
	void consume(Node n) throws ShutdownException, InterruptedException;
}
