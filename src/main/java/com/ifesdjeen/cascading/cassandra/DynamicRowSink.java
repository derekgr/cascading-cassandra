package com.ifesdjeen.cascading.cassandra;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.FieldsResolverException;
import org.apache.cassandra.thrift.*;

import com.ifesdjeen.cascading.cassandra.hadoop.CassandraHelper;

import java.io.IOException;
import java.util.*;
import java.nio.ByteBuffer;

import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamicRowSink
    implements ISink {

    private static final Logger logger = LoggerFactory.getLogger(DynamicRowSink.class);

    public List<Mutation> sink( Map<String, Object> settings,
                                TupleEntry tupleEntry ) {

        String keyColumnName = (String) settings.get("sink.keyColumnName");

        Map<String, String> wideMappings = (Map<String, String>) settings.get("sink.outputWideMappings");
        String columnNameField = (String) wideMappings.get("columnName");
        String columnValueField = (String) wideMappings.get("columnValue");

        Integer ttl = (Integer)settings.get("sink.columnTtl");

        List<Mutation> mutations = new ArrayList<Mutation>();

        Object tupleEntryColumnNameValue = null;
        try {
            tupleEntryColumnNameValue = tupleEntry.get(columnNameField);
        } catch (FieldsResolverException e) {
            logger.error("Couldn't resolve column name field: {}", columnNameField);
        }

        Object tupleEntryColumnValueValue = null;
        try {
            tupleEntryColumnValueValue = tupleEntry.get(columnValueField);
        } catch (FieldsResolverException e) {
            logger.error("Couldn't resolve column value field: {}", columnValueField);
        }

        if (tupleEntryColumnNameValue != null && tupleEntryColumnNameValue != keyColumnName) {
          if (logger.isDebugEnabled()) {
            logger.debug("Mapped column name field {}", columnNameField);
            logger.debug("column name value {}", tupleEntryColumnNameValue);
            logger.debug("Mapped column value field {}", columnValueField);
            logger.debug("Column value value {}", tupleEntryColumnValueValue);
          }

          Mutation mutation = Util.createColumnPutMutation(CassandraHelper.serialize(tupleEntryColumnNameValue),
                                                           CassandraHelper.serialize(tupleEntryColumnValueValue),
                                                           ttl);
          mutations.add(mutation);
        }

        return mutations;
    }



}
