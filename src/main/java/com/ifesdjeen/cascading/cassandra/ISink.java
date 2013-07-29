package com.ifesdjeen.cascading.cassandra;

import cascading.tuple.TupleEntry;
import org.apache.cassandra.thrift.*;
import java.util.*;

import java.nio.ByteBuffer;


public interface ISink {
    List<Mutation> sink( Map<String, Object> settings,
                         TupleEntry tupleEntry );
  public static class Util {
    public static Mutation createColumnPutMutation(ByteBuffer name, ByteBuffer value) {
      return createColumnPutMutation(name, value, 0);
    }

    public static Mutation createColumnPutMutation(ByteBuffer name, ByteBuffer value, int ttl) {
      Column column = new Column(name);
      column.setName(name);
      column.setValue(value);
      column.setTimestamp(System.currentTimeMillis());
      if (ttl > 0) {
        column.setTtl(ttl);
      }

      Mutation m = new Mutation();
      ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
      columnOrSuperColumn.setColumn(column);
      m.setColumn_or_supercolumn(columnOrSuperColumn);

      return m;
    }
  }
}
