package niagara.xmlql_parser;

/**
 *
 * base class for representing construct part
 *
 */

import niagara.optimizer.colombia.Attrs;

public abstract class constructBaseNode {
	// replace the occurences of variables with their corresponding
	// schema attributes
	abstract public void replaceVar(varTbl vt);

	// to print to the standard output
	abstract public void dump(int n);

	abstract public Attrs requiredInputAttributes(Attrs attrs);
}
