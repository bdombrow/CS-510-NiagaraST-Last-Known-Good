/* $Id: Pattern.java,v 1.1 2002/12/10 01:18:27 vpapad Exp $ */
package niagara.optimizer.rules;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import niagara.connection_server.Catalog;
import niagara.connection_server.ConfigurationError;
import niagara.optimizer.colombia.Expr;
import niagara.optimizer.colombia.LeafOp;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.Op;
import niagara.optimizer.colombia.PhysicalOp;
import niagara.utils.PEException;


abstract public class Pattern {
    /** An optional unique identifier for this pattern node.*/
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
