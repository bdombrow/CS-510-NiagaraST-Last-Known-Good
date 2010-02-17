package niagara.optimizer.rules;

import niagara.optimizer.colombia.LogicalOp;

/** Operators initializable by logical operators */
public interface Initializable {
	void initFrom(LogicalOp op);
}
