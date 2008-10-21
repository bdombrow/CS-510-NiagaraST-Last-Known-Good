/* $Id: Selecto.java,v 1.1 2008/10/21 23:11:39 rfernand Exp $ */
package niagara.logical;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.Vector;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.logical.predicates.*;
import niagara.optimizer.colombia.*;

/** The <code>Select</code> operator does a selection - so much for
 * useful comments! */

public class Selecto extends UnaryOperator {

    /** Number of tuples to allow through */
    private Predicate pred;

    private String special;
    
    public Selecto() {
    }

    public Selecto(Predicate pred, String special){
	this.pred = pred;
	this.special = special;
    }

    public Selecto(Selecto selOp, String special) {
        this(selOp.pred,special);
    }

    public Op opCopy() {
        Selecto op = new Selecto();
        op.pred = this.pred;
        op.special = this.special;
        return op;
    }

    /**
     * print the operator to the standard output
     */
    public void dump() {
	System.out.println("Selecto :");
	pred.dump(1);
    }

    public String toString() {
        return " selecto " + pred.toString();
    }

    public void dumpAttributesInXML(StringBuffer sb) {
        sb.append(" ");
    }

    public void dumpChildrenInXML(StringBuffer sb) {
        sb.append(">");
        pred.toXML(sb);
        sb.append("</selecto>");
    }

    public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog)
        throws InvalidPlanException {
        
    	special = e.getAttribute("special");
        
    	NodeList children = e.getChildNodes();
        Element predElt = null;
        for (int i = 0; i < children.getLength(); i++) {
            if (children.item(i) instanceof Element) {
                predElt = (Element) children.item(i);
                break;
            }
        }
	pred = Predicate.loadFromXML(predElt, inputProperties);
    }

    public LogicalProperty findLogProp(
        ICatalog catalog,
        LogicalProperty[] input) {
        LogicalProperty result = input[0].copy();
        result.setCardinality(result.getCardinality() * pred.selectivity());
        return result;
    }

    /**
     * @see niagara.xmlql_parser.op_tree.op#requiredInputAttributes(Attrs)
     */
    public Attrs requiredInputAttributes(Attrs inputAttrs) {
        ArrayList al = new ArrayList();
        pred.getReferencedVariables(al);
        Attrs reqd = new Attrs(al);
        assert inputAttrs.contains(reqd);
        return reqd;
    }

    public boolean equals(Object o) {
        if (o == null || !(o instanceof Selecto))
	    return false;
        if (o.getClass() != Selecto.class) 
	    return o.equals(this);
	Selecto s = (Selecto) o;
        return pred.equals(s.pred);
    }

    public int hashCode() {
        return pred.hashCode();
    }

    public Predicate getPredicate() {
	return pred;
    }

    public String getSpecial() {
    	return special;
    }
    
    public boolean isEmpty() {
        return pred.equals(True.getTrue());
    }
    
    /* Can we combine the selection with this unnesting? */   
    public boolean isPushableInto(Unnest unnest) {
        ArrayList al = new ArrayList();
        pred.getReferencedVariables(al);
        return (al.size() == 1 && ((Variable) al.get(0)).equals(unnest.getVariable()));
    }

   /**
    * used to set the predicate of Select operator. A list of predicate are
    * ANDed to produce single predicate for this operator
    *
    * @param list of predicates
    * KT - this is used by the trigger_engine *@#$*
    */
   public void setSelect(Vector _preds) {
	pred = niagara.xmlql_parser.Util.andPredicates(_preds);
   }

}
