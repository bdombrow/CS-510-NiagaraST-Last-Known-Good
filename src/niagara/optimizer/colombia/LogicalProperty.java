package niagara.optimizer.colombia;

/** Logical properties */
public class LogicalProperty {
    // XXX vpapad: is cardinality supposed to be -1 if unknown?
    // XXX vpapad: see Group.touchCopyCost
    float cardinality; 
    Attrs Attrs; // The list of attributes in the schema

    // Does this expression reference local resources?
    boolean hasLocal;
    // Does this expression reference remote resources?
    boolean hasRemote;
    
    public LogicalProperty(float card, Attrs attrs, boolean isLocal) {
        this.cardinality = card;
        this.Attrs = attrs.copy();

        assert card >= 0;

        if (attrs == null)
            Attrs = new Attrs();

        this.hasLocal = isLocal;
        this.hasRemote = !isLocal;
    }

    /** Initialize a logical property with a Get Operator */
    LogicalProperty(LogicalProperty LogProp, ATTR attr) {
        cardinality = LogProp.getCardinality();
        assert cardinality >= 0;
        Attrs = new Attrs(attr);// attr is the only attribute at this point
        hasLocal = LogProp.hasLocal;
        hasRemote = LogProp.hasRemote;
    }

    LogicalProperty(LogicalProperty other) { // copy constructor
        cardinality = other.getCardinality();

        assert cardinality >= 0;

        Attrs = new Attrs();
        for (int i = 0; i < other.GetAttrs().size(); i++)
            Attrs.add(other.GetAttrs().GetAt(i).copy());

        hasLocal = other.hasLocal;
        hasRemote = other.hasRemote;
    }

    public LogicalProperty copy() {
        return new LogicalProperty(this);
    }

    //Access Functions
    public float getCardinality() {
        return cardinality;
    }

    public Attrs GetAttrs() {
        return Attrs;
    }
    
    public void addAttr(ATTR a) {
        Attrs.add(a);
    }
    
    public void setCardinality(float c) {
        assert c >= 0;
        cardinality = c;
    }

    public void SetAttrs(Attrs t) {
        Attrs = t;
    }

    public boolean isLocal() {
        return hasLocal && !hasRemote;
    }
    
    public boolean isRemote() {
        return hasRemote;
    }
    
    public boolean isMixed() {
        return hasLocal && hasRemote;
    }
    
    public boolean hasLocal() {
        return hasLocal;
    }
    
    public boolean hasRemote() {
        return hasRemote;
    }
    
    public void setHasLocal(boolean local) {
        this.hasLocal = local;
    }

    public void setHasRemote(boolean remote) {
        this.hasRemote = remote;
    }

    Strings GetAttrNames() {
        return Attrs.GetAttrNames();
    }

    ATTR GetAttr(int i) {
        return Attrs.get(i);
    }

    public ATTR GetAttr(String attrname) {
        return Attrs.GetAttr(attrname);
    }

    boolean Contains(ATTR attr) {
        return Attrs.Contains(attr);
    }

    public boolean Contains(Attrs attrs) {
        return Attrs.Contains(attrs);
    }

    public boolean Contains(Strings attrnames) {
        return Attrs.Contains(attrnames);
    }

    /** number of attributes */
    public int getDegree() {
        return Attrs.size();
    } 
}
