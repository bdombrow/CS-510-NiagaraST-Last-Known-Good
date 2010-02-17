package niagara.logical;

import niagara.logical.path.RE;
import niagara.logical.predicates.Atom;

/**
 * A set of XML nodes that match a path starting at a variable
 */
public class Path implements Atom {
	private Variable var;
	private RE path;

	public Path(Variable var, RE path) {
		this.var = var;
		this.path = path;
	}

	public void toXML(StringBuffer sb) {
		sb.append("<path var=\"").append(var.getName()).append("\" regexp=\"")
				.append(path.toString()).append("\"/>");
	}

	public Variable getVar() {
		return var;
	}

	public RE getPath() {
		return path;
	}

	public boolean isConstant() {
		return false;
	}

	public boolean isVariable() {
		return false;
	}

	public boolean isPath() {
		return true;
	}
}
