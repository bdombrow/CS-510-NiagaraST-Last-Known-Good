/* $Id: SchedulablePlan.java,v 1.1 2002/10/06 23:56:41 vpapad Exp $ */
package niagara.query_engine;

import niagara.data_manager.DataManager;
import niagara.optimizer.colombia.Op;
import niagara.utils.ShutdownException;
import niagara.utils.SinkTupleStream;
import niagara.xmlql_parser.op_tree.op;

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
    void processSource(SinkTupleStream stream, DataManager dm)
    throws ShutdownException;
    
    int getNumberOfOutputs();
}
