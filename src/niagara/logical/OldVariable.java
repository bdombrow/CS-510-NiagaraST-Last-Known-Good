/* $Id: OldVariable.java,v 1.1 2002/09/20 23:14:20 vpapad Exp $ */
package niagara.logical;

import niagara.query_engine.AtomicEvaluator;
import niagara.xmlql_parser.syntax_tree.schemaAttribute;

public class OldVariable extends Variable {
    /** This class is for compatibility with the old  logical data structures, 
     * where variables were immediately translated to schema attributes. 
     * Not to be used with new code. */
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
