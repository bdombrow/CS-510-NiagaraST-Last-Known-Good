/* $Id: Variable.java,v 1.5 2003/02/25 06:13:16 vpapad Exp $ */
package niagara.logical;

import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Domain;
import niagara.query_engine.AtomicEvaluator;
import niagara.query_engine.SimpleAtomicEvaluator;
import niagara.xmlql_parser.syntax_tree.regExp;

public class Variable implements Atom, Attribute {
    private String name;
    private Domain domain;

    public Variable(String name, Domain domain) {
        this.name = name;
        this.domain = domain;
    }

    public Variable(String name) {
        this(name, NodeDomain.getDOMNode());
    }

    public Variable(String name, int type) {
        this(name, NodeDomain.getDOMNode(type));
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
        return
            name.equals(((Variable) other).getName()) && 
            domain.equals(((Variable) other).getDomain());
    }

    String Dump() {
        return " Name:" + name + " Domain:" + domain.getName();
    }

    public Attribute copy() {
        return new Variable(name, domain);
    }
    
    public int hashCode() {
        return name.hashCode() ^ domain.hashCode();
    }
}
