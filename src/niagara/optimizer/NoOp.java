/* $Id: NoOp.java,v 1.1 2002/12/10 01:18:27 vpapad Exp $ */
package niagara.optimizer;

import niagara.optimizer.colombia.*;
import niagara.optimizer.rules.Initializable;
import niagara.xmlql_parser.op_tree.unryOp;

/** Do nothing operator */
public class NoOp extends unryOp implements Initializable {
    public Op copy() {
        return this;
    }

    public NoOp() {}


    public void dump() {System.out.println("NoOp");}
    
    public boolean equals(Object other) {
        return this == other;
    }
    
    public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
        return input[0].copy();
    }
    
    public int hashCode() {
        return 0;
    }
    
    public void initFrom(LogicalOp op) {
        // Nothing to do
    }
}