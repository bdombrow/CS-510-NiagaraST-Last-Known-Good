/* $Id: SourceThread.java,v 1.2 2002/10/30 01:41:26 vpapad Exp $ */
package niagara.data_manager;

import niagara.connection_server.NiagraServer;
import niagara.optimizer.colombia.*;
import niagara.optimizer.rules.Initializable;
import niagara.query_engine.SchemaProducer;
import niagara.utils.SerializableToXML;
import niagara.utils.SinkTupleStream;

/** The leaves of a physical operator tree, distinct from regular
 * physical operators. */
public abstract class SourceThread 
    extends PhysicalOp implements Runnable, Initializable, SchemaProducer, SerializableToXML {
    public abstract void plugIn(SinkTupleStream outputStream, DataManager dm);
    public String getName() {
        return NiagraServer.getCatalog().getOperatorName(getClass());
    }
    
    public int getArity() { return 0; }
    
    /**
     * @see niagara.optimizer.colombia.PhysicalOp#FindPhysProp(PhysicalProperty[])
     */
    public PhysicalProperty FindPhysProp(PhysicalProperty[] input_phys_props) {
        return PhysicalProperty.ANY;
    }

    /**
     * @see niagara.optimizer.colombia.PhysicalOp#InputReqdProp(PhysicalProperty, LogicalProperty, int)
     */
    public PhysicalProperty[] InputReqdProp(
        PhysicalProperty PhysProp,
        LogicalProperty InputLogProp,
        int InputNo) {
            if (PhysProp.equals(PhysicalProperty.ANY))  
                return new PhysicalProperty[] {};    
            return null;
    }
}
