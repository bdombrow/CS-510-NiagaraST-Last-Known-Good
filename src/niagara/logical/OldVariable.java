/* $Id: OldVariable.java,v 1.3 2002/12/10 01:21:22 vpapad Exp $ */
package niagara.logical;

import niagara.query_engine.AtomicEvaluator;
import niagara.query_engine.SimpleAtomicEvaluator;
import niagara.utils.PEException;
import niagara.xmlql_parser.syntax_tree.regExp;
import niagara.xmlql_parser.syntax_tree.schemaAttribute;

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
