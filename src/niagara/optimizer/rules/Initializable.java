/* $Id: Initializable.java,v 1.2 2003/09/12 20:56:33 vpapad Exp $ */
package niagara.optimizer.rules;

import niagara.optimizer.colombia.LogicalOp;

/** Operators initializable by logical operators */
public interface Initializable {
    void initFrom(LogicalOp op);
}
