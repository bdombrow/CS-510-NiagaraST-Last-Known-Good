/* $Id */
package niagara.logical;

import niagara.xmlql_parser.op_tree.unryOp;
import niagara.xmlql_parser.syntax_tree.*;

public abstract class IncrementalGroup extends unryOp {
    // The skolem attributes associated with the group operator
    protected skolem skolemAttributes;

    public IncrementalGroup() {}
    
    public IncrementalGroup(skolem skolemAttributes) {
        this.skolemAttributes = skolemAttributes;
    }
    
    public void setSkolemAttributes(skolem skolemAttributes) {
	this.skolemAttributes = skolemAttributes;
    }

    public skolem getSkolemAttributes() {
	return skolemAttributes;
    }
}

