/* $Id: Variable.java,v 1.9 2003/09/16 04:53:35 vpapad Exp $ */
package niagara.logical;

import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Domain;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.query_engine.AtomicEvaluator;
import niagara.query_engine.SimpleAtomicEvaluator;
import niagara.xmlql_parser.syntax_tree.regExp;
import niagara.xmlql_parser.syntax_tree.varType;

public class Variable implements Atom, Attribute {
    private String name;
    private Domain domain;

    public Variable(String name, Domain domain) {
        // We want unique names for variables
        this.name = name.intern();
        this.domain = domain;
        if (domain == null)
            this.domain = NodeDomain.getDomain(varType.NULL_VAR);
    }

    public Variable(String name) {
        this(name, NodeDomain.getDOMNode());
    }

    public Variable(String name, int type) {
        this(name, NodeDomain.getDOMNode(type));
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

    public String getName() {
        return name;
    }

    public SimpleAtomicEvaluator getSimpleEvaluator() {
        return new SimpleAtomicEvaluator(name);
    }

    public AtomicEvaluator getEvaluator() {
        return new AtomicEvaluator(name);
    }

    public AtomicEvaluator getEvaluator(regExp path) {
        return new AtomicEvaluator(name, path);
    }

    public void toXML(StringBuffer sb) {
        sb.append("<var value='$").append(name).append("'/>");
    }

    public boolean isConstant() {
        return false;
    }

    public boolean isVariable() {
        return true;
    }

    public Domain getDomain() {
        return domain;
    }

    public boolean equals(Object other) {
        if (other == null || !(other instanceof Variable))
            return false;
        if (other.getClass() != Variable.class)
            return other.equals(this);
        return equals((Variable) other);
    }

    public boolean equals(Attribute other) {
        return name == other.getName()
            && domain.equals(other.getDomain());
    }
    
    public Attribute copy() {
        return new Variable(name, domain);
    }

    public int hashCode() {
        return name.hashCode() ^ domain.hashCode();
    }

    public boolean matchesName(String name) {
        return this.name.equals(name);
    }
}
