/* $Id: True.java,v 1.2 2003/09/16 04:53:35 vpapad Exp $ */
package niagara.logical;

import java.util.ArrayList;

import niagara.query_engine.PredicateImpl;
import niagara.query_engine.TrueImpl;

/* The constant true predicate */
public class True extends Predicate {
    // There is only one truth!
    private static final True t = new True();
    
    /** True is a singleton */
    private True() {}
    
    public static True getTrue() {
        return t;
    }
    
    public PredicateImpl getImplementation() {
        return TrueImpl.getTrueImpl();
    }

    public void getReferencedVariables(ArrayList al) {}
    
    public void beginXML(StringBuffer sb) {
        sb.append("<true/>");
    }
    
    public void childrenInXML(StringBuffer sb) {}
    public void toXML(StringBuffer sb) {}
    
    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object obj) {
        return this == obj;
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return System.identityHashCode(this);
    }

    public Predicate negation() {
        return False.getFalse();
    }

    public float selectivity() {
        return 1;
    }
}
