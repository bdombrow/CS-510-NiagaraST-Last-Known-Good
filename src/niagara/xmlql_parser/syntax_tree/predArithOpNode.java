
/**********************************************************************
  $Id: predArithOpNode.java,v 1.3 2001/07/17 06:53:29 vpapad Exp $


  NIAGARA -- Net Data Management System                                 
                                                                        
  Copyright (c)    Computer Sciences Department, University of          
                       Wisconsin -- Madison                             
  All Rights Reserved.                                                  
                                                                        
  Permission to use, copy, modify and distribute this software and      
  its documentation is hereby granted, provided that both the           
  copyright notice and this permission notice appear in all copies      
  of the software, derivative works or modified versions, and any       
  portions thereof, and that both notices appear in supporting          
  documentation.                                                        
                                                                        
  THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY    
  OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS "        
  AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND         
  FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.   
                                                                        
  This software was developed with support by DARPA through             
   Rome Research Laboratory Contract No. F30602-97-2-0247.  
**********************************************************************/


/**
 *
 * This class is used for representing arithmetic operators like '<', '<=',
 * '>', etc. in a predicate tree. This class extends predicate which stores
 * the operator, while this class stores the operands.
 *
 *
 */
package niagara.xmlql_parser.syntax_tree;


import org.w3c.dom.*;
import java.util.*;

public class predArithOpNode extends predicate {

    private data lexp;   // left operand (variable, identifier, etc. )
    private data rexp;   // right operand (   -- do --)

    /**
     * Constructor
     *
     * every predicate node stores the list of variable encountered in the
     * subtree rooted at it. For this class, it checks if its operands are
     * variables and if yes add them to the list of variables.
     *
     * @param the arithmetic operator
     * @param left operand
     * @param right operand
     */

    public predArithOpNode(int op, data lc, data rc) {
        super(op);
        lexp = lc;
        rexp = rc;

        String var;
        if(lexp.getType() == dataType.VAR) {
            var = (String)lexp.getValue();
            if(!varList.contains(var))
                varList.addElement(var);
        }
        if(rexp.getType() == dataType.VAR) {
            var = (String)rexp.getValue();
            if(!varList.contains(var))
                varList.addElement(var);
        }
    }

    /**
     * to replace the occurence of variables by its schemaAttribute from a
     * table that maps variables to schemaAttribute which captures the position
     * of the element in the schema.
     *
     * @param the variable table
     */

    public void replaceVar(varTbl tableofvar) {
        schemaAttribute sa;

        if(lexp.getType() == dataType.VAR) {
            sa = tableofvar.lookUp((String)lexp.getValue());
            if(sa != null)
                lexp = new data(dataType.ATTR,sa);
            else
                System.out.println("this shouldn't happen");
        }
        if(rexp.getType() == dataType.VAR) {
            sa = tableofvar.lookUp((String)rexp.getValue());
            if(sa != null)
                rexp = new data(dataType.ATTR,sa);
            else
                System.out.println("this shouldn't happen");
        }
    }

    /**
     * If the predicate contains variables from two different InClause 
     * ( as in join), then two variable list are given for the replacement.
     * Variables from the left stream get a stream id of 0, while that from
     * the right stream gets an id of 1. 
     *
     * @param variable table for the left stream
     * @param variable table for the right stream
     */

    public void replaceVar(varTbl leftVarTbl, varTbl rightVarTbl) {
        schemaAttribute sa;
        schemaAttribute new_sa;
        String var;

	// for the left operand
        if(lexp.getType() == dataType.VAR) {
            var = (String)lexp.getValue();
            sa = leftVarTbl.lookUp(var);
            if(sa != null) {
                new_sa = new schemaAttribute(sa);
                new_sa.setStreamId(0);
                lexp = new data(dataType.ATTR,new_sa);
            }
            else {
                sa = rightVarTbl.lookUp(var);
                if(sa != null) {
                    new_sa = new schemaAttribute(sa);
                    new_sa.setStreamId(1);
                    lexp = new data(dataType.ATTR,new_sa);
                }
                else
                    System.out.println("this shouldn't happen");
            }
        }

	// for the right operand
        if(rexp.getType() == dataType.VAR) {
            var = (String)rexp.getValue();
            sa = leftVarTbl.lookUp(var);
            if(sa != null) {
                new_sa = new schemaAttribute(sa);
                new_sa.setStreamId(0);
                rexp = new data(dataType.ATTR,new_sa);
            }
            else {
                sa = rightVarTbl.lookUp(var);
                if(sa != null) {
                    new_sa = new schemaAttribute(sa);
                    new_sa.setStreamId(1);
                    rexp = new data(dataType.ATTR,new_sa);
                }
                else
                    System.out.println("this shouldn't happen");
            }
        }
    }


    /**
     * @return left operand
     */

    public data getLeftExp() {
        return lexp;
    }

    /**
     * @return right operand
     */

    public data getRightExp() {
        return rexp;
    }

    // XXX horrible hack
    public void setVarList(Vector varList) {
        this.varList = varList;
    }


    public String dumpChildrenInXML() {
        String lxml, rxml;
        int varCnt = 0;
        
        if (lexp.type == dataType.NUMBER)
            lxml = "<number value='" + lexp.value + "'/>";
        else if (lexp.type == dataType.STRING || lexp.type == dataType.IDEN) 
            lxml = "<string value='" + lexp.value + "'/>";
        else {
            lxml = "<var value='" + varList.get(0) + "'/>";
            varCnt++;
        }
        if (rexp.type == dataType.NUMBER)
            rxml = "<number value='" + rexp.value + "'/>";
        else if (rexp.type == dataType.STRING) 
            rxml = "<string value='" + rexp.value + "'/>";
        else {
            rxml = "<var value='" + varList.get(varCnt) + "'/>";
        }
        return lxml + rxml;
    }

    /**
     * prints this to the standard output
     *
     * @param number of tabs at the beginning of each line
     */
    
    public void dump(int depth) {
        Util.genTab(depth);
        System.out.println("Arith Node");
        Util.genTab(depth);

        switch(getOperator()) {
            case opType.EQ: System.out.println("="); break;
            case opType.NEQ: System.out.println("!="); break;
            case opType.LEQ: System.out.println("<="); break;
            case opType.GEQ: System.out.println(">="); break;
            case opType.GT: System.out.println(">"); break;
            case opType.LT: System.out.println("<"); break;
        }
        Util.genTab(depth);
        System.out.println("[left]");
        lexp.dump(depth+1);
        Util.genTab(depth);
        System.out.println("[right]");
        rexp.dump(depth+1);
    }
}
