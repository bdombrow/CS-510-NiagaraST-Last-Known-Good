/* $Id: Strings.java,v 1.4 2003/02/25 06:19:08 vpapad Exp $
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

import java.util.ArrayList;

/** A container for strings, typically used for attribute names. 
 * Functions as a set or a list, depending on context */
public class Strings {
    private ArrayList attrnames;

    public Strings() {
        attrnames = new ArrayList();
    }

    public Strings(Strings other) {
        attrnames.addAll(other.attrnames);
    }
    public boolean isEmpty() {
        return attrnames.isEmpty();
    }
    public boolean notEmpty() {
        return !attrnames.isEmpty();
    }

    public int size() {
        return attrnames.size();
    }

    public String get(int pos) {
        return (String) attrnames.get(pos);
    }

    public void add(String s) {
        attrnames.add(s);
    }
    // Check whether attrname is in this set
    public boolean contains(String attrname) {
        for (int i = 0; i < attrnames.size(); i++) {
            // XXX vpapad: changing == to equals options: equals,
            // ==, keep an extra hashtable, intern strings, ...
            if (attrnames.get(i).equals(attrname))
                return true;
        }
        return false;
    }

    // Check whether attrnames is subset of this set
    public boolean contains(Strings other) {
        if (size() < other.size())
            return false;
        for (int i = 0; i < other.size(); i++) {
            if (!(contains(other.get(i))))
                return false;
        }
        return true;
    }

    // Check whether there is overlap between this and other
    boolean overlap(Strings other) {
        for (int i = 0; i < other.size(); i++) {
            if (contains(other.get(i)))
                return true;
        }
        for (int i = 0; i < size(); i++) {
            if (other.contains(get(i)))
                return true;
        }
        return false;
    }

    // Check if they are equivalet SETS
    boolean equivalent(Strings other) {
        if (size() != other.size())
            return false;
        for (int i = 0; i < size(); i++) {
            if (!(contains(other.get(i))))
                return false;
        }
        return true;
    }

    /** return a copy of this object keeping only the attributes in other*/ 
    Strings projection(Strings other) {
        Strings attrs = new Strings();
        for (int i = size() - 1; i >= 0; i--) {
            if (other.contains(get(i)))
                attrs.add(get(i));
        }
        return attrs;
    }

    public Strings copy() {
        return new Strings(this);
    }

    // XXX vpapad: why is this here?!?
    //     void attachPathName(String varname) {
    // 	for (int i=0; i<size(); i++) {
    // 	    assert((*this)[i] != "");
    // 	    (*this)[i] =  varname + "." + (*this)[i];
    // 	}
    //     }

    int findIdx(String s) {
        return attrnames.lastIndexOf(s);
    }

    // XXX vpapad: commented out from the C++ version
    // Remove duplicates
    //boolean Add(String c)
    //{
    //	if (Contains(c)) return false;
    //	InsertAt(size(), c);
    //	return true;
    //}

    void addAll(Strings other) {
        for (int i = 0; i < other.size(); i++)
            if (!(contains(other.get(i))))
                attrnames.add(other.get(i));
    }

    // Same as AddAll unless duplicates are not removed
    void concat(Strings other) {
        attrnames.addAll(other.attrnames);
    }

    // Remove an element 
    public boolean remove(String str) {
        int index = attrnames.lastIndexOf(str);
        if (index >= 0) {
            attrnames.remove(index);
            return true;
        }

        return false;
    }

    void distinct() {
        // XXX vpapad: Horribly slow
        ArrayList temp = new ArrayList(attrnames.size());

        for (int i = 0; i < size(); i++) {
            if (!temp.contains(attrnames.get(i)))
                temp.add(attrnames.get(i));
        }
        attrnames.clear();
        attrnames.addAll(temp);
    }


    public boolean equals(Object other) {
        if (other == null || !(other instanceof Strings))
            return false;
        if (other.getClass() != Strings.class)
            return other.equals(this);
        Strings o = (Strings) other;
        if (size() != o.size()) 
            return false;
        for (int i = 0; i < attrnames.size(); i++) 
            if (!attrnames.get(i).equals(o.attrnames.get(i)))
                return false;
        return true;
    }
    
    public int hashCode() {
        int hashCode = 0;
        for (int i = 0; i < attrnames.size(); i++) {
            hashCode ^= attrnames.get(i).hashCode();
        }
        return hashCode;
    }
    
    String dump(String delimiter) {
        if (size() == 0)
            return "";

        StringBuffer sb = new StringBuffer();
        sb.append(attrnames.get(0));
        for (int i = 1; i < size(); i++) {
            sb.append(delimiter);
            sb.append(attrnames.get(i));
        }
        return sb.toString();
    }

    String dump() {
        return dump(", ");
    }

    String dumpNewLine() {
        return dump("\n");
    }
}
