/* $Id: TupleSchema.java,v 1.1 2002/10/06 23:56:42 vpapad Exp $ */
package niagara.query_engine;

import java.util.*;

import niagara.optimizer.colombia.ATTR;
import niagara.optimizer.colombia.Attrs;

/** A tuple schema maps attribute/variable names to tuple positions. */
public class TupleSchema {
    /** Length of the tuple */
    private int length;

    /** Maps variable name to tuple position */    
    private HashMap name2pos;

    /** Maps variable name to variable */
    private HashMap name2var;
    
    /** Maps tuple position to variable name */    
    private HashMap pos2name;
    
    public TupleSchema() {
        length = 0;
        name2pos = new HashMap();
        name2var = new HashMap();
        pos2name = new HashMap();
    }

    /** Deep copy of this tuple schema */
    public TupleSchema copy() {
        TupleSchema ts = new TupleSchema();
        for (int i = 0; i < length; i++) {
            ATTR attr = getVariable(i);
            ts.addMapping(attr);
        }
        return ts;
    }
    
    public void addMappings(Attrs attrs) {
        for (int i = 0; i < attrs.size(); i++) {
            addMapping(attrs.get(i));
        }
    }
    
    /** Map a name to a new field */
    public void addMapping(ATTR var) {
        String name = var.getName();
        assert name2pos.get(name) == null : "Duplicate variable name";
        Integer pos = new Integer(length);
        length++;
        name2pos.put(name, pos);
        name2var.put(name, var);
        pos2name.put(pos, name);
    }
        
    public int getLength() {
        return length;
    }

    public boolean contains(String name) {
        return (name2pos.get(name) != null);
    }
    
    public int getPosition(String name) {
        return ((Integer) name2pos.get(name)).intValue();
    }

    public ATTR getVariable(int position) {
        return (ATTR) name2var.get(getVariableName(position));
    }
        
    public String getVariableName(int position) {
        return (String) pos2name.get(new Integer(position));
    }
    
    public ArrayList getVariables() {
        ArrayList al = new ArrayList(length);
        for (int i = 0; i < length; i++) {
            al.add(getVariable(i));
        }
        return al;
    }
}
