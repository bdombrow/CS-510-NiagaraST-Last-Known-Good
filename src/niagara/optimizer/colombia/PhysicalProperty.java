package niagara.optimizer.colombia;

/**
   PhysicalProperty: PHYSICAL PROPERTIES
*/

// Physical properties of collections.  These properites 
// distinguish collections which are logically equivalent.  Examples
// are orderings, data distribution, data compression, etc.

// Normally, a plan could have > 1 physical property.  For now,
//  we will work with hashing and sorting only, so we assume a plan
//  can have only one physical property.  Extensions should be
//  tedious but not too hard.

public class PhysicalProperty {
    // Physical properties are immutable objects
       
    /** A physical property that guarantees nothing at all */
    public static PhysicalProperty ANY = new PhysicalProperty(Order.newAny());

    private Order order;

    public PhysicalProperty(Order order) {
        this.order = order;
    }

    public PhysicalProperty copy() {
        return new PhysicalProperty(order);
    }

    PhysicalProperty(PhysicalProperty other) {
        order = other.getOrder();
    }

    public boolean equals(PhysicalProperty other) {
        return (other.getOrder().equals(order));
    }

    Order.Kind getOrderKind() {
        return order.getKind();
    }

    public Strings getOrderAttrNames() {
        return order.getAttrNames();
    }

    String getOrderAttrName() {
        return order.getAttrName();
    }

    public String toString() {
        return order.toString();
    }

    public Order getOrder() {
        return order;
    }
}
