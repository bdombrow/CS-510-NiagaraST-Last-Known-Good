/* $Id: OldVariable.java,v 1.4 2003/12/24 02:08:31 vpapad Exp $ */
package niagara.logical;

import niagara.physical.AtomicEvaluator;
import niagara.physical.SimpleAtomicEvaluator;
import niagara.utils.PEException;
import niagara.xmlql_parser.regExp;
import niagara.xmlql_parser.schemaAttribute;

public class OldVariable extends Variable {
    /** This class is for compatibility with the old  logical data structures, 
     * where variables were immediately translated to schema attributes. 
     * Not to be used with new code. */
    
    public OldVariable(String name) {
        super(name);
        this.sa = null;
    }
    
    public OldVariable(schemaAttribute sa) {
        super(null);
        this.sa = sa;
    }
    
    public void setSA(schemaAttribute sa) {
        this.sa = sa;
    }
    
    private schemaAttribute sa;

    public schemaAttribute getSA() {
        return sa;
    }

    /**
     * @see niagara.logical.Variable#getEvaluator()
     */
    public AtomicEvaluator getEvaluator() {
        return new AtomicEvaluator(sa);
    }
    /**
     * @see niagara.logical.Variable#getEvaluator(regExp)
     */
    public AtomicEvaluator getEvaluator(regExp path) {
        throw new PEException("Old style schema attributes already have a path attribute");
    }

    /**
     * @see niagara.logical.Variable#getSimpleEvaluator()
     */
    public SimpleAtomicEvaluator getSimpleEvaluator() {
        return new SimpleAtomicEvaluator(sa);
    }
}
