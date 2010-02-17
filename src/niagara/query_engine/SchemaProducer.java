package niagara.query_engine;

/** Producers of tuple schemas */
public interface SchemaProducer {
	void constructTupleSchema(TupleSchema[] inputSchemas);

	TupleSchema getTupleSchema();
}
