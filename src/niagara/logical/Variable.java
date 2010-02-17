package niagara.logical;

import niagara.connection_server.InvalidPlanException;
import niagara.logical.path.RE;
import niagara.logical.predicates.Atom;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Domain;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.physical.AtomicEvaluator;
import niagara.physical.SimpleAtomicEvaluator;
import niagara.utils.BaseAttr;
import niagara.xmlql_parser.varType;

public class Variable implements Atom, Attribute {
	private String name;
	private Domain domain;
	private BaseAttr.Type dataType;

	public Variable(String name, Domain domain) {
		// We want unique names for variables
		this.name = name.intern();
		this.domain = domain;
		this.dataType = BaseAttr.Type.XML;
		if (domain == null)
			this.domain = NodeDomain.getDomain(varType.NULL_VAR);
	}

	public Variable(String name) {
		this(name, NodeDomain.getDOMNode());
	}

	public Variable(String name, int type) {
		this(name, NodeDomain.getDOMNode(type));
	}

	public Variable(String name, BaseAttr.Type dataType) {
		this.name = name;
		this.dataType = dataType;
		this.domain = NodeDomain.getDOMNode();
	}

	public Variable(String name, BaseAttr.Type dataType, int type) {
		this.name = name;
		this.dataType = dataType;
		this.domain = NodeDomain.getDOMNode(type);
	}

	public Variable(String name, BaseAttr.Type dataType, Domain domain) {
		this.name = name;
		this.dataType = dataType;
		this.domain = domain;
		if (domain == null)
			this.domain = NodeDomain.getDomain(varType.NULL_VAR);

	}

	public static Attribute findVariable(LogicalProperty lp, String varName)
			throws InvalidPlanException {
		// Strip dollar signs from varName
		if (varName.charAt(0) == '$')
			varName = varName.substring(1);

		Attribute attr = lp.getAttr(varName);
		if (attr == null)
			throw new InvalidPlanException("Unknown variable: " + varName);
		return attr;
	}

	public static Attribute findVariable(LogicalProperty[] props, String varName)
			throws InvalidPlanException {
		// Strip dollar signs from varName
		if (varName.charAt(0) == '$')
			varName = varName.substring(1);

		for (int i = 0; i < props.length; i++) {
			Attribute attr = props[i].getAttr(varName);
			if (attr != null)
				return attr;
		}
		throw new InvalidPlanException("Unknown variable: " + varName);
	}

	public String getName() {
		return name;
	}

	public SimpleAtomicEvaluator getSimpleEvaluator() {
		return new SimpleAtomicEvaluator(name);
	}

	public AtomicEvaluator getEvaluator() {
		return new AtomicEvaluator(name);
	}

	public AtomicEvaluator getEvaluator(RE path) {
		return new AtomicEvaluator(name, path);
	}

	public void toXML(StringBuffer sb) {
		sb.append("<var value='$").append(name).append("'/>");
	}

	public String toString() {
		return getName();
	}

	public boolean isConstant() {
		return false;
	}

	public boolean isVariable() {
		return true;
	}

	public boolean isPath() {
		return false;
	}

	public Domain getDomain() {
		return domain;
	}

	public BaseAttr.Type getDataType() {
		return dataType;
	}

	public boolean equals(Object other) {
		if (other == null || !(other instanceof Variable))
			return false;
		if (other.getClass() != Variable.class)
			return other.equals(this);
		return equals((Variable) other);
	}

	public boolean equals(Attribute other) {
		// return name.equals(other.getName());

		if (dataType == BaseAttr.Type.XML)
			if (!domain.equals(other.getDomain()))
				return false;
		return name.equals(other.getName());
		// return name.equals(other.getName()) && dataType ==
		// other.getDataType();

		/*
		 * return name == other.getName() && domain.equals(other.getDomain());
		 */
	}

	public Attribute copy() {
		// return new Variable (name, dataType);
		// return new Variable(name, domain);
		return new Variable(name, dataType, domain);
	}

	public int hashCode() {
		// return name.hashCode() ^ domain.hashCode();
		return name.hashCode() ^ dataType.hashCode();
	}

	public boolean matchesName(String name) {
		return this.name.equals(name);
	}

	public Attribute copy(String newName) {
		// return new Variable(newName, domain);
		// return new Variable(newName, dataType);
		return new Variable(newName, dataType, domain);
	}
}
