/* $Id: Attrs.java,v 1.4 2002/12/10 01:18:26 vpapad Exp $ */
package niagara.optimizer.colombia;
import java.util.ArrayList;

/*
    ============================================================
    SET OF ATTRIBUTES - class Attrs
    //Used for free attributes, materialized attributes, and in Schema.  
    ============================================================
*/

public class Attrs {

    ArrayList al;

    public Attribute get(int i) {
        return (Attribute) al.get(i);
    }

    public Attrs() {
        al = new ArrayList();
    }

    public Attrs(int size) {
        al = new ArrayList(size);
    }

    public Attrs(Attribute attr) {
        this();
        al.add(attr);
    }

    Attrs(Attribute[] array, int size) {
        al.ensureCapacity(size);
        for (int i = 0; i < size; i++)
            al.add(array[i]);
    }

    public Attrs(ArrayList list) {
        this();
        al.addAll(list);
    }
    
    Attrs(Attrs other) { // copy constructor
        this();
        al.addAll(other.al);
    }

    Strings GetAttrNames() {
        Strings result = new Strings();

        for (int i = 0; i < size(); i++)
            result.add(get(i).getName());
        return result;
    }

    Attribute GetAttr(String name) {
        for (int i = 0; i < size(); i++)
            if (get(i).getName().equals(name))
                return get(i);
        return null;
    }

    public int size() {
        return al.size();
    }

    Attribute GetAt(int i) {
        return (Attribute) al.get(i);
    }

    public void add(Attribute attr) {
        al.add(attr);
    }

    public void merge(Attrs other) {
        for (int i = 0; i < other.size(); i++) {
            if (!Contains(other.get(i)))
                add(other.get(i));
        }
    }

    // Check if the attribute is in the Attrs
    // Two attributes with the same name are considered the same
    boolean Contains(String attrName) {
        // check if the attid is in the vector
        for (int i = 0; i < size(); i++)
            if (attrName.equals(get(i).getName()))
                return true;

        return false;
    }

    // Check if the attribute is in the Attrs
    // Two attributes with the same name are considered the same
    boolean Contains(Strings AttrNames) {
        for (int i = 0; i < AttrNames.size(); i++)
            if (!Contains(AttrNames.get(i)))
                return false;

        return true; // this Attrs is contained in array
    }

    public boolean Contains(Attribute Attr) {
        return Contains(Attr.getName());
    }

    public boolean Contains(ArrayList variables) {
        for (int i = 0; i < variables.size(); i++) {
            Attribute a = (Attribute) variables.get(i);
            if (!Contains(a)) return false;
        }
        
        return true;
    }
    
    public boolean contains(Attrs other) {
        for (int i = 0; i < other.size(); i++)
            if (!Contains(other.get(i)))
                return false;

        return true; // this Attrs is contained in array
    }

    boolean IsSubset(Attrs other) {
        return other.contains(this);
    }

    boolean IsOverlapped(Attrs other) {
        for (int i = 0; i < size(); i++)
            for (int j = 0; j < other.size(); j++)
                if (get(i).getName() == other.get(j).getName())
                    return true;
        return false;
    }
    boolean IsOverlapped(Strings other) {
        for (int i = 0; i < size(); i++)
            for (int j = 0; j < other.size(); j++)
                if (get(i).getName() == other.get(j))
                    return true;
        return false;
    }

    // New Attrs with just the attributes in "attrs" 
    public Attrs project(Attrs attrs) {
        Attrs result = new Attrs();
        for (int i = 0; i < size(); i++)
            if (attrs.Contains(get(i)))
                result.add(get(i));
        return result;
    }

    // New Attrs without the attributes in "attrs"
    public Attrs minus(Attrs attrs) {
        Attrs result = new Attrs();
        for (int i = 0; i < size(); i++)
            if (!attrs.Contains(get(i)))
                result.add(get(i));
        return result;
    }

    // Remove an element from Attrs
    boolean RemoveAttr(Attribute Attr) {
        int i;
        for (i = 0; i < size() - 1; i++)
            if (get(i).getName() == Attr.getName())
                break;
        if (i == size())
            return false;
        al.remove(i);
        return true;
    }

    // Remove an element from Attrs
    boolean RemoveAttr(String AttrName) {
        int i;
        for (i = 0; i < size() - 1; i++)
            if (get(i).getName().equals(AttrName))
                break;
        if (i == size())
            return false;
        al.remove(i);
        return true;
    }

    public Attrs copy() {
        Attrs result = new Attrs();
        // check duplicate element in vector
        for (int i = 0; i < size(); i++)
            result.add(get(i).copy());
        return result;
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof Attrs))
            return false;
        if (obj.getClass() != Attrs.class)
            return obj.equals(this);
        Attrs other = (Attrs) obj;
        if (al.size() != other.al.size()) return false;
        for (int i = 0; i < al.size(); i++) {
            if (!al.get(i).equals(other.al.get(i))) return false;
        }
        return true;
    }

    public int hashCode() {
        int hash = 0;
        for (int i = 0; i < al.size(); i++) {
            hash ^= al.get(i).hashCode();
        }
        return hash;
    }
    
    public String toString() {
        if (al.size() == 0) return "";
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < al.size() - 1; i++)
            sb.append(((Attribute) al.get(i)).getName()).append(",");
            
        sb.append(((Attribute) al.get(al.size() - 1)).getName());
        return sb.toString();
    }
}
