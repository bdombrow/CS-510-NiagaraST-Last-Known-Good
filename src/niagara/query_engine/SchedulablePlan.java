/* $Id: SchedulablePlan.java,v 1.5 2003/12/24 01:31:46 vpapad Exp $ */
package niagara.query_engine;

import niagara.data_manager.DataManager;
import niagara.physical.*;
import niagara.utils.ShutdownException;
import niagara.utils.SinkTupleStream;

/** A SchedulablePlan is an executable DAG of query operators */
public interface SchedulablePlan {
    PhysicalOperator getPhysicalOperator();
    
    int getArity();
    
    SchedulablePlan getInput(int i);
    
    void setIsHead();
    boolean isHead();

    /** A unique identifier for this plan node */   
    String getName();
    
    boolean isSchedulable();
    
    boolean isSource();
    void processSource(SinkTupleStream stream, DataManager dm, PhysicalOperatorQueue opQueue)
    throws ShutdownException;
    
    int getNumberOfOutputs();

    boolean isSendImmediate();
    void setSendImmediate();
}
