/* $Id: Not.java,v 1.1 2002/10/06 23:40:12 vpapad Exp $ */
package niagara.logical;

import java.util.ArrayList;

import niagara.query_engine.NotImpl;
import niagara.query_engine.PredicateImpl;

/** Negation of a predicate */
public class Not extends Predicate {
    private Predicate pred;
    
    public Not(Predicate pred) {
        this.pred = pred;
    }

    public PredicateImpl getImplementation() {
        return new NotImpl(pred.getImplementation());
    }
    
        public void beginXML(StringBuffer sb) {
        sb.append("<not>");
    }
    
    public void childrenInXML(StringBuffer sb) {
        pred.toXML(sb);
    }
    
    public void endXML(StringBuffer sb) {
        sb.append("</not>");
    }
    
    /**
     * @see niagara.logical.Predicate#getReferencedVariables(ArrayList)
     */
    public void getReferencedVariables(ArrayList al) {
        pred.getReferencedVariables(al);
    }
    
    /**
     * @see niagara.logical.Predicate#copy()
     */
    public Predicate copy() {
        return new Not(pred);
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object other) {
        if (other == null || !(other instanceof Not))
            return false;
        if (other.getClass() != Not.class)
            return other.equals(this);
        return pred.equals(((Not) other).pred);
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return pred.hashCode();
    }

    /**
     * @see niagara.logical.Predicate#negate()
     */
    Predicate negation() {
        return pred;
    }
    /**
     * @see niagara.logical.Predicate#selectivity()
     */
    public float selectivity() {
        return 1 - pred.selectivity();
    }
}