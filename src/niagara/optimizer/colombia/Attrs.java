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

    public ATTR get(int i) {
        return (ATTR) al.get(i);
    }

    public Attrs() {
        al = new ArrayList();
    }

    public Attrs(int size) {
        al = new ArrayList(size);
    }

    public Attrs(ATTR attr) {
        this();
        al.add(attr);
    }

    Attrs(ATTR[] array, int size) {
        al.ensureCapacity(size);
        for (int i = 0; i < size; i++)
            al.add(array[i]);
    }

    public Attrs(ArrayList list) {
        this();
        al.ensureCapacity(list.size());
        for (int i = 0; i < list.size(); i++) {
            al.add(list.get(i));
        }
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

    ATTR GetAttr(String name) {
        for (int i = 0; i < size(); i++)
            if (get(i).getName().equals(name))
                return get(i);
        return null;
    }

    public int size() {
        return al.size();
    }

    ATTR GetAt(int i) {
        return (ATTR) al.get(i);
    }

    public void add(ATTR attr) {
        al.add(attr);
    }

    // merge keys, ignore duplicates
    // should there be duplicate?
    public void Merge(Attrs other) {
        for (int i = 0; i < other.size(); i++)
            add(other.get(i));
    }

    // Check if the attribute is in the Attrs
    // Two attributes with the same name are considered the same
    boolean Contains(String AttrName) {
        // check if the attid is in the vector
        for (int i = 0; i < size(); i++)
            if (AttrName == get(i).getName())
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

    public boolean Contains(ATTR Attr) {
        return Contains(Attr.getName());
    }

    public boolean Contains(ArrayList variables) {
        for (int i = 0; i < variables.size(); i++) {
            ATTR a = (ATTR) variables.get(i);
            if (!Contains(a)) return false;
        }
        
        return true;
    }
    
    public boolean Contains(Attrs other) {
        for (int i = 0; i < other.size(); i++)
            if (!Contains(other.get(i)))
                return false;

        return true; // this Attrs is contained in array
    }

    boolean IsSubset(Attrs other) {
        return other.Contains(this);
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

    //remove the attributes that are not in the "attrs" list. 
    Attrs Projection(Strings AttrNames) {
        Attrs attrs = new Attrs();
        for (int i = size() - 1; i >= 0; i--)
            if (AttrNames.contains(get(i).getName()))
                attrs.add(get(i));
        return attrs;
    }

    //remove the attributes that are not in the "attrs" list. 
    Attrs Projection(Attrs attrs) {
        Attrs result = new Attrs();
        for (int i = 0; i < size(); i++)
            if (attrs.Contains(get(i)))
                result.add(get(i));
        return result;
    }

    // return int array of size one from the keys_set
    //int * CopyOutOne(int i);

    // Remove an element from Attrs
    boolean RemoveAttr(ATTR Attr) {
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
    /**
     * @see java.lang.Object#equals(Object)
     */
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

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        int hash = 0;
        for (int i = 0; i < al.size(); i++) {
            hash ^= al.get(i).hashCode();
        }
        return hash;
    }
}
