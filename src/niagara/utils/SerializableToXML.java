/* $Id$ */
package niagara.utils;

/** An interface for structures that can be serialized to XML format */
public interface SerializableToXML {
    void dumpAttributesInXML(StringBuffer sb);
    void dumpChildrenInXML(StringBuffer sb);
}
