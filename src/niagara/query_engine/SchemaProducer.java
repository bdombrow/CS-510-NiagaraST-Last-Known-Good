/* $Id: SchemaProducer.java,v 1.1 2002/10/06 23:56:41 vpapad Exp $ */
package niagara.query_engine;

/** Producers of tuple schemas */
public interface SchemaProducer {
    void constructTupleSchema(TupleSchema[] inputSchemas);
    TupleSchema getTupleSchema();
}
