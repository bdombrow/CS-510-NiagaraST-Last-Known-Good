package niagara.optimizer;

import niagara.logical.UnaryOperator;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.optimizer.rules.Initializable;

/** Do nothing operator */
public class NoOp extends UnaryOperator implements Initializable {
	public Op opCopy() {
		return this;
	}

	public NoOp() {
	}

	public void dump() {
		System.out.println("NoOp");
	}

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
