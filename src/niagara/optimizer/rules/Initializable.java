/* $Id: Initializable.java,v 1.1 2002/10/23 21:54:27 vpapad Exp $ */
package niagara.optimizer.rules;

import niagara.optimizer.colombia.LogicalOp;

/** Operators initializable by logical operators */
public interface Initializable {
    void initFrom(LogicalOp op);
}
