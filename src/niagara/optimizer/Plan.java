/* $Id$ */
package niagara.optimizer;

import java.text.DecimalFormat;
import java.util.*;

import niagara.data_manager.DataManager;
import niagara.data_manager.SourceThread;
import niagara.optimizer.colombia.Expr;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.Op;
import niagara.optimizer.colombia.PhysicalOp;
import niagara.query_engine.PhysicalOperator;
import niagara.query_engine.SchedulablePlan;
import niagara.query_engine.SchemaProducer;
import niagara.query_engine.TupleSchema;
import niagara.utils.PEException;
import niagara.utils.SerializableToXML;
import niagara.utils.SinkTupleStream;
import niagara.xmlql_parser.op_tree.ResourceOp;
import niagara.xmlql_parser.op_tree.op;

public class Plan implements SchedulablePlan {
    private Op operator;
    private Plan[] inputs;
    private boolean isHead;

    /** A plan node is schedulable if none of its inputs depends 
     * on an abstract resource */
    private boolean isSchedulable;

    private double cost;
    private String name;

    public Plan(Op operator) {
        this.operator = operator;
        this.inputs = new Plan[] {
        };
        if (operator instanceof op) 
            isSchedulable = ((op) operator).isSchedulable();
        else
            isSchedulable = true;
    }

    public Plan(Op operator, Plan oneInput) {
        this.operator = operator;
        this.inputs = new Plan[] { oneInput };
        this.isSchedulable = oneInput.isSchedulable;
    }

    public Plan(Op operator, Plan leftInput, Plan rightInput) {
        this.operator = operator;
        this.inputs = new Plan[] { leftInput, rightInput };
        this.isSchedulable = leftInput.isSchedulable && rightInput.isSchedulable;
    }

    public Plan(Op operator, Plan[] inputs) {
        this.operator = operator;
        this.inputs = inputs;
        isSchedulable = true;
        for (int i = 0; i < inputs.length; i++)
            isSchedulable &= inputs[i].isSchedulable;
    }

    public static Plan getPlan(Expr expr, ICatalog catalog) {
        return new Plan(expr, new HashMap(), catalog);
    }

    protected Plan(Expr expr, HashMap hm, ICatalog catalog) {
        hm.put(expr, this);
        operator = expr.getOp();
        inputs = new Plan[expr.getArity()];
        if (operator.isPhysical())
            setCost(expr.getCost(catalog).getValue());
        isSchedulable = (! (operator instanceof ResourceOp));
        for (int i = 0; i < inputs.length; i++) {
            Expr e = expr.getInput(i);
            if (hm.containsKey(e))
                inputs[i] = (Plan) hm.get(e);
            else
                inputs[i] = new Plan(expr.getInput(i), hm, catalog);
            isSchedulable &= inputs[i].isSchedulable;
        }
    }

    public Expr toExpr() {
        return toExpr(new HashMap());
    }

    private Expr toExpr(HashMap seen) {
        Expr[] inputExprs = new Expr[inputs.length];
        for (int i = 0; i < inputs.length; i++) {
            if (seen.containsKey(inputs[i]))
                inputExprs[i] = (Expr) seen.get(inputs[i]);
            else
                inputExprs[i] = inputs[i].toExpr(seen);
        }
        Expr e = new Expr(operator, inputExprs);
        seen.put(this, e);
        return e;
    }
    
    public ResourceOp getResource() {
        return (ResourceOp) operator;
    }
    
    public Op getOperator() {
        return operator;
    }
    
    /**
     * @see niagara.query_engine.SchedulablePlan#getPhysicalOperator()
     */
    public PhysicalOperator getPhysicalOperator() {
        PhysicalOperator pop;
        if (operator.isPhysical()) {
	    // KT - will this croak if operator is a SourceThread??
            pop = (PhysicalOperator) operator;
	} else
            throw new PEException("unexpected request for physical op");

        // Construct the tuple schema, if it's not there        
        if (pop.getTupleSchema() == null) {
            TupleSchema[] tupleSchemas = new TupleSchema[inputs.length];
            for (int i = 0; i < inputs.length; i++) {
                tupleSchemas[i] =
                    ((SchemaProducer) inputs[i].operator).getTupleSchema();
            }
            pop.constructTupleSchema(tupleSchemas);
        }

        return pop;
    }

    public TupleSchema getTupleSchema() {
        SchemaProducer sp;
        if (!(operator instanceof SchemaProducer))
            throw new PEException("unexpected request for schema producer");
        sp = (SchemaProducer) operator;
        return sp.getTupleSchema();
    }
    
    /**
     * @see niagara.query_engine.SchedulablePlan#getArity()
     */
    public int getArity() {
        return inputs.length;
    }

    /**
     * @see niagara.query_engine.SchedulablePlan#getInput(int)
     */
    public SchedulablePlan getInput(int i) {
        return inputs[i];
    }

    public void replaceWith(Plan other) {
        this.operator = other.operator;
        this.inputs = other.inputs;
    }
    
    public void setOperator(Op operator) {
        this.operator = operator;
    }
    
    public void setInputs(Plan[] inputs) {
        this.inputs = inputs;
    }
    
    public void setInput(int i, Plan p) {
        inputs[i] = p;
    }

    /**
     * @see niagara.query_engine.SchedulablePlan#setIsHead()
     */
    public void setIsHead() {
        isHead = true;
    }

    /**
     * @see niagara.query_engine.SchedulablePlan#isHead()
     */
    public boolean isHead() {
        return isHead;
    }

    /**
     * @see niagara.query_engine.SchedulablePlan#setSendImmed()
     */
    public void setSendImmediate() {
	assert operator.isPhysical() : "SendImmediate is a physical property";
	((PhysicalOp)operator).setSendImmediate();
    }

    /**
     * @see niagara.query_engine.SchedulablePlan#isSendImmed()
     */
    public boolean isSendImmediate() {
	assert operator.isPhysical() : "SendImmediate is a physical property";
        return ((PhysicalOp)operator).isSendImmediate();
    }

    /**
     * @see niagara.query_engine.SchedulablePlan#getName()
     */
    public String getName() {
        if (name != null)
            return name;
        // otherwise return an artificial unique name 
        return operator.getName() + "#" + Math.abs(operator.hashCode());
    }

    /**
     * @see niagara.query_engine.SchedulablePlan#isSchedulable()
     */
    public boolean isSchedulable() {
        return isSchedulable;
    }

    /**
     * @see niagara.query_engine.SchedulablePlan#isSource()
     */
    public boolean isSource() {
        return (operator.getArity() == 0);
    }

    public void processSource(SinkTupleStream stream, DataManager dm) {
        assert isSource() : "not a source op";
        SourceThread thread = (SourceThread) operator;
        thread.plugIn(stream, dm);
        (new Thread(thread)).start();
    }

    /**
     * @see niagara.query_engine.SchedulablePlan#getNumberOfOutputs()
     */
    public int getNumberOfOutputs() {
        return operator.getNumberOfOutputs();
    }

    public void assignNames(HashMap hm) {
        if (name != null)
            return;
        // Assign names to children first
        for (int i = 0; i < inputs.length; i++)
            inputs[i].assignNames(hm);

        String opName = operator.getName();
        Integer count = (Integer) hm.get(opName);
        if (count == null)
            count = new Integer(0);
        count = new Integer(count.intValue() + 1);
        name = opName + count;
        hm.put(opName, count);
    }

    /**
     * XML representation of this node
     *
     * @return a <code>String</code> with the XML 
     * representation of this operator
     */
    public void toXML(StringBuffer sb, DecimalFormat df) {
        sb.append("<").append(operator.getName());
        sb.append(" id='").append(getName()).append("'");
        // inputs
        if (inputs.length != 0) {
            sb.append(" input='").append(inputs[0].getName());
            for (int i = 1; i < inputs.length; i++) {
                sb.append(" ").append(inputs[i].getName());
            }
            sb.append("'");
        }
        sb.append(" cost='").append(df.format(cost)).append("' ");
        ((SerializableToXML) operator).dumpAttributesInXML(sb);
        ((SerializableToXML) operator).dumpChildrenInXML(sb);
        sb.append("\n");
    }

    public String planToXML() {
        assignNames(new HashMap());
        StringBuffer buf = new StringBuffer("<plan top='");
        buf.append(getName());
        buf.append("'>\n");
        subplanToXML(new Hashtable(), buf, new DecimalFormat());
        buf.append("</plan>");
        return buf.toString();
    }

    public String subplanToXML() {
        StringBuffer buf =
            new StringBuffer(
                "<plan top='send'><send id='send' input='" + getName() + "'/>");
        subplanToXML(new Hashtable(), buf, new DecimalFormat());
        buf.append("</plan>");
        return buf.toString();
    }

    public void subplanToXML(
        Hashtable seen,
        StringBuffer buf,
        DecimalFormat df) {
        String name = getName();
        if (seen.containsKey(name))
            return;
        toXML(buf, df);
        seen.put(name, name);
        for (int i = 0; i < inputs.length; i++) {
            inputs[i].subplanToXML(seen, buf, df);
        }
    }

    public void getRootsAndResources(
        HashSet visited,
        ArrayList roots,
        ArrayList resources,
        boolean lookingForRoot) {
        // XXX vpapad: If a plan node is an input to both
        // a schedulable plan and an unschedulable plan, it
        // will be executed twice!
        if (visited.contains(this))
            return;
        visited.add(this);
        if (operator instanceof ResourceOp)
            resources.add(this);
        else if (isSchedulable && lookingForRoot) {
            roots.add(this);
            lookingForRoot = false;
        }
            
        for (int i = 0; i < inputs.length; i++)
            inputs[i].getRootsAndResources(visited, roots, resources, lookingForRoot);
    }

    public void setCost(double cost) {
        this.cost = cost;
    }

    public double getCost() {
        return cost;
    }
}
