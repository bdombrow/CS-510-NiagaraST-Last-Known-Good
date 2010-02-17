package niagara.logical.predicates;

/** Atoms participate in comparisons */
public interface Atom {
	void toXML(StringBuffer sb);

	boolean isConstant();

	boolean isVariable();

	boolean isPath();
}
