package com.ifesdjeen.cascading.cassandra;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.FieldsResolverException;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;

import com.ifesdjeen.cascading.cassandra.hadoop.CassandraHelper;

import java.io.IOException;
import java.util.*;
import java.nio.ByteBuffer;

import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Outputs to Cassandra rows with composite columns.
 * Like StaticRowSink, you provide output mappings for sink columns
 * with sink.outputMappings.
 * You also indicate which tuples in the result are the composite columns by
 * passing an array as sink.compositeColumns, and you provide their
 * types in sink.outputColumnTypes.
 */
public class CompositeRowSink implements ISink {
    private static final Logger logger = LoggerFactory.getLogger(CompositeRowSink.class);

    public List<Mutation> sink( Map<String, Object> settings,
                                TupleEntry tupleEntry ) {

        Map<String, String> fieldMappings = (Map<String, String>) settings.get("sink.outputMappings");
        String keyColumnName = (String) settings.get("sink.keyColumnName");
        List<String> composites = (List<String>) settings.get("sink.compositeColumns");
        List<AbstractType<?>> types = (List<AbstractType<?>>) settings.get("sink.compositeColumnTypes");
        Set<String> columnFieldNames = (Set<String>)fieldMappings.keySet();

        int nfields = columnFieldNames.size() - composites.size();
        List<Mutation> mutations = new ArrayList<Mutation>(nfields);

        ArrayList compositeValues = new ArrayList();
        for (String columnFieldName : composites) {
            String columnFieldMapping = fieldMappings.get(columnFieldName);
            Object tupleEntryValue = null;

            try {
                tupleEntryValue = tupleEntry.get(columnFieldMapping);
                compositeValues.add(tupleEntryValue);
            } catch (FieldsResolverException e) {
                logger.error("Couldn't resolve field: {}", columnFieldName);
            }
        }

        for (String columnFieldName : columnFieldNames) {
            String columnFieldMapping = fieldMappings.get(columnFieldName);
            Object tupleEntryValue = null;

            if (columnFieldName.equals(keyColumnName)
                || composites.contains(columnFieldName)) {
              continue;
            }

            try {
                tupleEntryValue = tupleEntry.get(columnFieldMapping);
            } catch (FieldsResolverException e) {
                logger.error("Couldn't resolve field: {}", columnFieldName);
            }

            if (tupleEntryValue != null) {
                Mutation mutation = Util.createColumnPutMutation(makeCompositeColumn(types, compositeValues, columnFieldName),
                                                                 CassandraHelper.serialize(tupleEntryValue));
                mutations.add(mutation);
            }
        }

        return mutations;
    }

    private ByteBuffer makeCompositeColumn(List<AbstractType<?>> types, List compositeValues, String columnName) {
      CompositeType col = CompositeType.getInstance(types);
      CompositeType.Builder builder = new CompositeType.Builder(col);
      for (Object val : compositeValues) {
          builder.add(CassandraHelper.serialize(val));
      }
      builder.add(CassandraHelper.serialize(columnName));
      return builder.build();
    }
}
