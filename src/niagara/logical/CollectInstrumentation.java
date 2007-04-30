/* $Id: CollectInstrumentation.java,v 1.1 2007/04/30 19:21:14 vpapad Exp $ */
package niagara.logical;

import org.w3c.dom.Element;

import java.util.ArrayList;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.*;

public class CollectInstrumentation extends NullaryOperator {
    /** Instrumented plan */
    private String plan;

    /** Operators we are collecting instrumentation from */
    private ArrayList<String> operators;
    
    /** Period, in milliseconds */
    private int period;

    public CollectInstrumentation() {
    }

    public CollectInstrumentation(String plan, ArrayList<String> operators, int period) {
        this.plan = plan;
        this.operators = operators;
        this.period = period;
    }

    public CollectInstrumentation(CollectInstrumentation op) {
        this(op.plan, op.operators, op.period);
    }

    public Op opCopy() {
        return new CollectInstrumentation(this);
    }

    /**
     * print the operator to the standard output
     */
    public void dump() {
        System.out.println(this);
    }

    public String toString() {
        return " collect instrumentation, plan =  " + plan + " operators = "
                + operators.toString() + ", period = " + period + "ms.";
    }

    public void dumpAttributesInXML(StringBuffer sb) {
        sb.append(" plan='").append(plan).append("' operators='");
        // Strip brackets from ArrayList.toString()
        String ops = operators.toString();
        sb.append(ops.substring(1, ops.length() - 1));
        sb.append("' period='").append(period).append("'");
    }

    public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
        Attrs attrs = new Attrs(); 
        attrs.add(new Variable("time"));
        attrs.add(new Variable("operator"));
        attrs.add(new Variable("name"));
        attrs.add(new Variable("value"));

        return new LogicalProperty(1, attrs, true);
    }

    public boolean equals(Object o) {
        if (o == null || !(o instanceof CollectInstrumentation))
            return false;
        if (o.getClass() != CollectInstrumentation.class)
            return o.equals(this);
        CollectInstrumentation c = (CollectInstrumentation) o;
        return plan.equals(c.plan) && operators.equals(c.operators)
                && period == c.period;
    }

    public int hashCode() {
        return plan.hashCode() ^ operators.hashCode() ^ period;
    }

    public void loadFromXML(Element e, LogicalProperty[] inputProperties,
            Catalog catalog) throws InvalidPlanException {
        plan = e.getAttribute("plan");
        if (plan.length() < 1)
            throw new InvalidPlanException(
                    "You must specify a plan to collect instrumentation values from.");

        String[] ops = e.getAttribute("operators").split(",");
        if (ops.length < 1)
            throw new InvalidPlanException(
                    "You must specify at least one operator to collect instrumentation values from.");
        operators = new ArrayList<String>();
        
        String sPeriod = e.getAttribute("period");
        try {
            period = Integer.parseInt(sPeriod);
        } catch (NumberFormatException nfe) {
            throw new InvalidPlanException("Expected period in milliseconds, got '" + sPeriod + "'");
        }
        
        for (String s : ops)
            operators.add(s);
    }

    public ArrayList<String> getOperators() {
        return operators;
    }

    public String getPlan() {
        return plan;
    }
    
    public int getPeriod() {
        return period;
    }
}
