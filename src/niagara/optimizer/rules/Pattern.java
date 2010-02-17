package niagara.optimizer.rules;

import niagara.optimizer.colombia.Expr;
import niagara.optimizer.colombia.Op;

abstract public class Pattern {
	/** An optional unique identifier for this pattern node. */
	protected String name;
	/** Operator for this pattern node. */
	protected Op operator;

	protected Pattern findByName(String name) {
		if (this.name.equals(name))
			return this;
		Pattern[] inputs = getInputs();
		for (int i = 0; i < inputs.length; i++) {
			Pattern found = inputs[i].findByName(name);
			if (found != null)
				return found;
		}

		return null;
	}

	public Expr toExpr() {
		Pattern[] inputs = getInputs();

		Expr[] inpExprs = new Expr[inputs.length];
		for (int i = 0; i < inputs.length; i++)
			inpExprs[i] = inputs[i].toExpr();
		return new Expr(operator, inpExprs);
	}

	public String getName() {
		return name;
	}

	public Op getOperator() {
		return operator;
	}

	abstract public Pattern[] getInputs();

	public Op followAddress(byte[] address, Expr e) {
		for (int i = 0; i < address.length; i++)
			e = e.getInput(address[i]);
		return e.getOp();
	}

}
