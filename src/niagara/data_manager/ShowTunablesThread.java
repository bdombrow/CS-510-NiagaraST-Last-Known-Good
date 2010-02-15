/**
 * $Id: ShowTunablesThread.java,v 1.1 2007/04/30 19:19:05 vpapad Exp $
 *
 */

package niagara.data_manager;

/** 
 * Show information about all tunable parameters in a plan
 */

import org.w3c.dom.*;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

import org.xml.sax.*;

import niagara.physical.Tunable;
import niagara.query_engine.*;
import niagara.utils.*;
import niagara.connection_server.Catalog;
import niagara.connection_server.NiagraServer;
import niagara.logical.ConstantScan;
import niagara.logical.ShowTunables;
import niagara.ndom.*;
import niagara.optimizer.Plan;
import niagara.optimizer.PlanVisitor;
import niagara.optimizer.colombia.*;

public class ShowTunablesThread extends SourceThread implements PlanVisitor {
    // Optimization-time attributes
    private String planID;

    private SinkTupleStream outputStream;

    private Document doc;

    public ShowTunablesThread() {
    };

    public ShowTunablesThread(String planID) {
        this.planID = planID;
    }

    public void plugIn(SinkTupleStream outputStream, DataManager dm) {
        this.outputStream = outputStream;
    }

    public void cleanUp(String msg) {
        try {
            if (msg != null)
                System.err.println("ShowTunablesThread: " + msg);
            // REFACTOR
            outputStream.putCtrlMsg(ControlFlag.SHUTDOWN, msg);
            planID = null;
            outputStream = null;
            doc = null;
        } catch (InterruptedException e) {
            ; // XXX vpapad: What are we supposed to do here? 
        } catch (ShutdownException e) {
            ; // XXX vpapad: What are we supposed to do here? 
        }
    }
    
    public void run() {
        Catalog catalog = NiagraServer.getCatalog();
        Plan plan = catalog.getPreparedPlan(planID);
        if (plan == null) {
            cleanUp("Unknown planID: " + planID);
            return;
        }

        doc = DOMFactory.newDocument();
        plan.visitAllNodes(this);

        try {
            outputStream.endOfStream();
        } catch (InterruptedException ie) {
            /* do nothing */
        } catch (ShutdownException se) {
            /* do nothing */
        }
    }

    public Cost findLocalCost(ICatalog catalog, LogicalProperty[] InputLogProp) {
        // XXX vpapad: Totally bogus
        return new Cost(42 * catalog.getDouble("tuple_construction_cost"));
    }

    public void opInitFrom(LogicalOp op) {
        ShowTunables st = (ShowTunables) op;
        this.planID = st.getPlanID();
    }

    public Op opCopy() {
        return new ShowTunablesThread(planID);
    }

    public int hashCode() {
        return hashCodeNullsAllowed(planID);
    }

    public boolean equals(Object o) {
        if (o == null || !(o instanceof ShowTunablesThread))
            return false;
        if (o.getClass() != getClass())
            return o.equals(this);
        return planID.equals(((ShowTunablesThread) o).planID);
    }

    public void constructTupleSchema(TupleSchema[] inputSchemas) {
        ; // Do nothing
    }

    public TupleSchema getTupleSchema() {
        TupleSchema ts = new TupleSchema();
        ts.addMappings(ShowTunables.getDefaultAttrs());
        return ts;
    }

    public void dumpAttributesInXML(StringBuffer sb) {
        sb.append(" planID='").append(planID).append("' schema='").append(
                getLogProp().getAttrs().toString()).append("'/>");
    }

    public void dumpChildrenInXML(StringBuffer sb) {
        ; // Do nothing
    }

    public boolean visit(Plan p) {
        StringAttr sa = new StringAttr(p.getName());
        Schedulable s = (Schedulable) p.getOperator();
        for (Method m : s.getClass().getMethods()) {
            if (!m.isAnnotationPresent(Tunable.class))
                continue;
            Tunable tunable = m.getAnnotation(Tunable.class);
            Tuple tuple = new Tuple(true, 5);
            tuple.setAttribute(0, sa);
            tuple.setAttribute(1, new StringAttr(tunable.name()));
            tuple.setAttribute(2, new StringAttr(tunable.type().toString()));
            tuple.setAttribute(3, new StringAttr(tunable.description()));
            try {
                tuple.setAttribute(4, new StringAttr(m.invoke(s).toString()));
            } catch (Exception e) {
                cleanUp("Problem with retrieving tunable information " + e.getMessage());
                return false;
            }
            
            try {
                outputStream.putTuple(tuple);
            } catch (InterruptedException e) {
                cleanUp(" interrupted: " + e.getMessage());
                ; // XXX vpapad: What else are we supposed to do here?
                return false;
            } catch (ShutdownException e) {
                cleanUp(" shutdown: " + e.getMessage());
                ; // XXX vpapad: What else are we supposed to do here?
                return false;
            }
        }
        return true;
    }
}
