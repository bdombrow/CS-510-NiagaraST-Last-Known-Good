/* $Id: Variable.java,v 1.1 2002/09/20 23:14:20 vpapad Exp $ */
package niagara.logical;

import java.util.ArrayList;

import niagara.optimizer.colombia.ATTR;
import niagara.optimizer.colombia.Domain;
import niagara.query_engine.AtomicEvaluator;

public class Variable implements Atom, ATTR {
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

    public AtomicEvaluator getEvaluator() {
        return new AtomicEvaluator(name);
    }

    public void toXML(StringBuffer sb) {
        sb.append("<var value='$");
        sb.append(name);
        sb.append("'/>");
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

    Variable Copy() {
        return new Variable(name, domain);
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


    /**
     * @see niagara.optimizer.colombia.ATTR#copy()
     */
    public ATTR copy() {
        return new Variable(name, domain);
    }
    
    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return name.hashCode()^ domain.hashCode();
    }
}