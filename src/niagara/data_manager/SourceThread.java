/* $Id: SourceThread.java,v 1.6 2007/04/30 19:19:05 vpapad Exp $ */
package niagara.data_manager;

import java.util.ArrayList;

import niagara.connection_server.NiagraServer;
import niagara.optimizer.colombia.*;
import niagara.optimizer.rules.Initializable;
import niagara.query_engine.Instrumentable;
import niagara.query_engine.Schedulable;
import niagara.query_engine.SchemaProducer;
import niagara.utils.SerializableToXML;
import niagara.utils.SinkTupleStream;

/** The leaves of a physical operator tree, distinct from regular
 * physical operators. For the optimizer, a SourceThread is a 
 * regular physical operator. */
public abstract class SourceThread 
    extends PhysicalOp implements Schedulable, Initializable, SchemaProducer, SerializableToXML, Instrumentable {
    public abstract void plugIn(SinkTupleStream outputStream, DataManager dm);
    public String getName() {
        return NiagraServer.getCatalog().getOperatorName(getClass());
    }
    
    public int getArity() { return 0; }

    public final void initFrom(LogicalOp op) {
	this.id = op.getId();
	opInitFrom(op);
    }

    // do local initialization
    protected abstract void opInitFrom(LogicalOp op);
    
    
    // Instrumentation
    public void getInstrumentationValues(ArrayList<String> names, 
            ArrayList<Object> values) {
        ; // No instrumentation for now
    }
    public void setInstrumented(boolean instrumented) {
        ; // No instrumentation for now
    }
}
