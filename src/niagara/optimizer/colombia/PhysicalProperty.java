/* $Id: PhysicalProperty.java,v 1.5 2003/06/03 07:56:51 vpapad Exp $
   Colombia -- Java version of the Columbia Database Optimization Framework

   Copyright (c)    Dept. of Computer Science , Portland State
   University and Dept. of  Computer Science & Engineering,
   OGI School of Science & Engineering, OHSU. All Rights Reserved.

   Permission to use, copy, modify, and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies
   of the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.

   THE AUTHORS, THE DEPT. OF COMPUTER SCIENCE DEPT. OF PORTLAND STATE
   UNIVERSITY AND DEPT. OF COMPUTER SCIENCE & ENGINEERING AT OHSU ALLOW
   USE OF THIS SOFTWARE IN ITS "AS IS" CONDITION, AND THEY DISCLAIM ANY
   LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE
   USE OF THIS SOFTWARE.

   This software was developed with support of NSF grants IRI-9118360,
   IRI-9119446, IRI-9509955, IRI-9610013, IRI-9619977, IIS 0086002,
   and DARPA (ARPA order #8230, CECOM contract DAAB07-91-C-Q518).
*/

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

    public Strings getOrderAttrNames() {
        return order.getAttrNames();
    }

    public String toString() {
        return order.toString();
    }

    public Order getOrder() {
        return order;
    }
}
