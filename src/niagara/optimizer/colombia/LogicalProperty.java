package niagara.optimizer.colombia;

/** Logical properties */
public class LogicalProperty {
    // XXX vpapad: is cardinality supposed to be -1 if unknown?
    // XXX vpapad: see Group.touchCopyCost
    float cardinality;
    private Attrs attrs; // The list of attributes in the schema

    // Does this expression reference local resources?
    boolean hasLocal;
    // Does this expression reference remote resources?
    boolean hasRemote;

    public LogicalProperty(float card, Attrs attrs, boolean isLocal) {
        this.cardinality = card;
        this.attrs = attrs.copy();

        assert card >= 0;

        if (attrs == null)
            this.attrs = new Attrs();

        this.hasLocal = isLocal;
        this.hasRemote = !isLocal;
    }

    /** Initialize a logical property with a Get Operator */
    LogicalProperty(LogicalProperty LogProp, Attribute attr) {
        cardinality = LogProp.getCardinality();
        assert cardinality >= 0;
        attrs = new Attrs(attr); // attr is the only attribute at this point
        hasLocal = LogProp.hasLocal;
        hasRemote = LogProp.hasRemote;
    }

    LogicalProperty(LogicalProperty other) { // copy constructor
        cardinality = other.getCardinality();

        assert cardinality >= 0;

        attrs = new Attrs();
        for (int i = 0; i < other.getAttrs().size(); i++)
            attrs.add(other.getAttrs().GetAt(i).copy());

        hasLocal = other.hasLocal;
        hasRemote = other.hasRemote;
    }

    public boolean equals(Object o) {
        if (o == null || !(o instanceof LogicalProperty))
            return false;
        if (o.getClass() != LogicalProperty.class)
            return o.equals(this);
        LogicalProperty lp = (LogicalProperty) o;
        return lp.cardinality == lp.getCardinality()
            && hasLocal == lp.hasLocal()
            && hasRemote == lp.hasRemote()
            && attrs.equals(lp.getAttrs());
    }

    public int hashCode() {
        return ((int) cardinality) ^ (hasLocal?1:0) ^ (hasRemote?2:0) ^ attrs.hashCode();
    }
    
    public LogicalProperty copy() {
        return new LogicalProperty(this);
    }

    //Access Functions
    public float getCardinality() {
        return cardinality;
    }

    public Attrs getAttrs() {
        return attrs;
    }

    public void addAttr(Attribute a) {
        attrs.add(a);
    }

    public void setCardinality(float c) {
        assert c >= 0;
        cardinality = c;
    }

    public void setAttrs(Attrs t) {
        attrs = t;
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
        return attrs.GetAttrNames();
    }

    Attribute GetAttr(int i) {
        return attrs.get(i);
    }

    public Attribute getAttr(String attrname) {
        return attrs.GetAttr(attrname);
    }

    boolean Contains(Attribute attr) {
        return attrs.Contains(attr);
    }

    public boolean Contains(Attrs attrs) {
        return this.attrs.contains(attrs);
    }

    public boolean Contains(Strings attrnames) {
        return attrs.Contains(attrnames);
    }

    /** number of attributes */
    public int getDegree() {
        return attrs.size();
    }
}
