/* $Id: OldVariable.java,v 1.2 2002/10/31 03:32:21 vpapad Exp $ */
package niagara.logical;

import niagara.query_engine.AtomicEvaluator;
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
}
