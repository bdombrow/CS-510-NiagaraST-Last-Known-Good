/* $Id: SourceThread.java,v 1.3 2002/10/31 03:26:48 vpapad Exp $ */
package niagara.data_manager;

import niagara.connection_server.NiagraServer;
import niagara.optimizer.colombia.*;
import niagara.optimizer.rules.Initializable;
import niagara.query_engine.SchemaProducer;
import niagara.utils.SerializableToXML;
import niagara.utils.SinkTupleStream;

/** The leaves of a physical operator tree, distinct from regular
 * physical operators. For the optimizer, a SourceThread is a 
 * regular physical operator. */
public abstract class SourceThread 
    extends PhysicalOp implements Runnable, Initializable, SchemaProducer, SerializableToXML {
    public abstract void plugIn(SinkTupleStream outputStream, DataManager dm);
    public String getName() {
        return NiagraServer.getCatalog().getOperatorName(getClass());
    }
    
    public int getArity() { return 0; }
}
