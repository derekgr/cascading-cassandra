package com.ifesdjeen.cascading.cassandra;

import com.ifesdjeen.cascading.cassandra.hadoop.SerializerHelper;
import cascading.tuple.FieldsResolverException;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;

import cascading.tap.Tap;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;

import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;

import com.ifesdjeen.cascading.cassandra.hadoop.CassandraHelper;

import java.io.IOException;
import java.util.*;
import java.nio.ByteBuffer;

import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.IColumn;

public class CassandraScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {

  private static final Logger logger = LoggerFactory.getLogger(CassandraTap.class);

  private String pathUUID;
  private Map<String, Object> settings;

  private String host;
  private String port;
  private String columnFamily;
  private String keyspace;

  // Use this constructor when using CassandraScheme as a Sink
  public CassandraScheme(Map<String, Object> settings) {
    this.settings = settings;
    this.pathUUID = UUID.randomUUID().toString();

    if (this.settings.containsKey("db.port")) {
      this.port = (String) this.settings.get("db.port");
    } else {
      this.port = "9160";
    }

    if (this.settings.containsKey("db.host")) {
      this.host = (String) this.settings.get("db.host");
    } else {
      this.host = "localhost";
    }

    this.keyspace = (String) this.settings.get("db.keyspace");
    this.columnFamily = (String) this.settings.get("db.columnFamily");
  }

  /**
   * @param flowProcess
   * @param sourceCall
   */
  @Override
  public void sourcePrepare(FlowProcess<JobConf> flowProcess,
                            SourceCall<Object[], RecordReader> sourceCall) {
    ByteBuffer key = ByteBufferUtil.clone((ByteBuffer) sourceCall.getInput().createKey());
    SortedMap<ByteBuffer, IColumn> value = (SortedMap<ByteBuffer, IColumn>) sourceCall.getInput().createValue();

    Object[] obj = new Object[]{key, value};

    sourceCall.setContext(obj);
  }

  /**
   * @param flowProcess
   * @param sourceCall
   */
  @Override
  public void sourceCleanup(FlowProcess<JobConf> flowProcess,
                            SourceCall<Object[], RecordReader> sourceCall) {
    sourceCall.setContext(null);
  }

  /**
   * @param flowProcess
   * @param sourceCall
   * @return
   * @throws IOException
   */
  @Override
  public boolean source(FlowProcess<JobConf> flowProcess,
                        SourceCall<Object[], RecordReader> sourceCall) throws IOException {
    RecordReader input = sourceCall.getInput();

    Object key = sourceCall.getContext()[0];
    Object value = sourceCall.getContext()[1];

    boolean hasNext = input.next(key, value);

    if (!hasNext) {
      return false;
    }

    SortedMap<ByteBuffer, IColumn> columns = (SortedMap<ByteBuffer, IColumn>) value;
    Tuple result = new Tuple();

    String keyType = (String)this.settings.get("source.keyType");
    if (keyType != null) {
      try {
        result.add(SerializerHelper.deserialize((ByteBuffer)key, keyType));
      }
      catch (ConfigurationException ex) {
        throw new IOException("Error deserializing key: " + key.toString(), ex);
      }
    }
    else {
      result.add(ByteBufferUtil.string((ByteBuffer) key));
    }

    Map<String, String> dataTypes = this.getSourceTypes();

    if (!dataTypes.isEmpty()) {
      if (columns.values().isEmpty()) {
        logger.info("Values are empty.");
      }

      for (IColumn column : columns.values()) {
        String columnName = ByteBufferUtil.string(column.name());
        if (dataTypes.containsKey(columnName)) {
          try {
            Object val = SerializerHelper.deserialize(column.value(), dataTypes.get(columnName));
            logger.debug("Putting deserialized column: {}. {}", columnName, val);
            result.add(val);
          } catch (Exception e) {
            logger.error("Couldn't deserialize column: {}. {}", columnName, e.getMessage());
          }
        } else {
          // Assuming wide rows here
          if ((Boolean) this.settings.get("source.useWideRows")) {
            try {
              Object val = SerializerHelper.deserialize(column.name(), dataTypes.get("key"));
              result.add(val);
              val = SerializerHelper.deserialize(column.value(), dataTypes.get("value"));
              result.add(val);
            } catch (Exception e) {}
          } else {
            logger.info("Skipping column, because there was no type given: {}", columnName);
          }
        }
      }
    } else {
      result.add(columns);
      logger.debug("No data types given. Assuming custom deserizliation.");
    }

    sourceCall.getIncomingEntry().setTuple(result);
    return true;

  }

  /**
   * @param flowProcess
   * @param sinkCall
   * @throws IOException
   */
  @Override
  public void sink(FlowProcess<JobConf> flowProcess,
                   SinkCall<Object[], OutputCollector> sinkCall) throws IOException {

    TupleEntry tupleEntry = sinkCall.getOutgoingEntry();
    OutputCollector outputCollector = sinkCall.getOutput();

    String keyColumnName = (String) this.settings.get("sink.keyColumnName");
    Map<String, String> fieldMappings = (Map<String, String>) this.settings.get("sink.outputMappings");

    Tuple key = tupleEntry.selectTuple(new Fields(fieldMappings.get(keyColumnName)));
    ByteBuffer keyBuffer = CassandraHelper.serialize(key.get(0));

    ISink sinkImpl = getSinkImpl( (String)this.settings.get("sink.sinkImpl") );

    List<Mutation> mutations = sinkImpl.sink(settings, tupleEntry);

    outputCollector.collect(keyBuffer, mutations);
  }

  /**
   * @param name
   * @param value
   * @return
   */
  protected Mutation createColumnPutMutation(ByteBuffer name, ByteBuffer value) {
    Column column = new Column(name);
    column.setName(name);
    column.setValue(value);
    column.setTimestamp(System.currentTimeMillis());

    Mutation m = new Mutation();
    ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
    columnOrSuperColumn.setColumn(column);
    m.setColumn_or_supercolumn(columnOrSuperColumn);

    return m;
  }

  @Override
  public void sinkConfInit(FlowProcess<JobConf> process,
                           Tap<JobConf, RecordReader, OutputCollector> tap,
                           JobConf conf) {
    conf.setOutputFormat(ColumnFamilyOutputFormat.class);

    ConfigHelper.setRangeBatchSize(conf, 1000);

    ConfigHelper.setOutputRpcPort(conf, port);
    ConfigHelper.setOutputInitialAddress(conf, host);

    if (this.settings.containsKey("cassandra.outputPartitioner")) {
       ConfigHelper.setOutputPartitioner(conf, (String) this.settings.get("cassandra.outputPartitioner"));
    } else {
       ConfigHelper.setOutputPartitioner(conf, "org.apache.cassandra.dht.Murmur3Partitioner");
    }

    ConfigHelper.setOutputColumnFamily(conf, keyspace, columnFamily);

    FileOutputFormat.setOutputPath(conf, getPath());
  }

  @Override
  public void sourceConfInit(FlowProcess<JobConf> process,
                             Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
    logger.info("Configuring source...");
    ConfigHelper.setInputRpcPort(conf, port);
    ConfigHelper.setInputInitialAddress(conf, this.host);


    if (this.settings.containsKey("source.rangeBatchSize")) {
      ConfigHelper.setRangeBatchSize(conf, (Integer) this.settings.get("source.rangeBatchSize"));
    } else {
      ConfigHelper.setRangeBatchSize(conf, 1000);
    }

    if (this.settings.containsKey("source.inputSplitSize")) {
      ConfigHelper.setInputSplitSize(conf, (Integer) this.settings.get("source.inputSplitSize"));
    }

    if (this.settings.containsKey("cassandra.inputPartitioner")) {
      ConfigHelper.setInputPartitioner(conf, (String) this.settings.get("cassandra.inputPartitioner"));
    } else {
      ConfigHelper.setInputPartitioner(conf, "org.apache.cassandra.dht.Murmur3Partitioner");
    }

    if (this.settings.containsKey("source.predicate")) {
      ConfigHelper.setInputSlicePredicate(conf, (SlicePredicate) this.settings.get("source.predicate"));
    } else {
      SlicePredicate predicate = new SlicePredicate();

      List<String> sourceColumns = this.getSourceColumns();

      if (!sourceColumns.isEmpty()) {
        logger.debug("Using with following columns: {}", StringUtils.join(sourceColumns, ","));

        List<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();
        for (String columnFieldName : sourceColumns) {
          columnNames.add(ByteBufferUtil.bytes(columnFieldName));
        }

        predicate.setColumn_names(columnNames);
      } else {
        logger.debug("Using slicerange over all columns");

        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(ByteBufferUtil.bytes(""));
        sliceRange.setFinish(ByteBufferUtil.bytes(""));
        predicate.setSlice_range(sliceRange);
      }
      ConfigHelper.setInputSlicePredicate(conf, predicate);
    }

    if (this.settings.containsKey("source.useWideRows")) {
      ConfigHelper.setInputColumnFamily(conf, this.keyspace, this.columnFamily,
              (Boolean) this.settings.get("source.useWideRows"));
    } else {
      ConfigHelper.setInputColumnFamily(conf, this.keyspace, this.columnFamily);
    }
    FileInputFormat.addInputPaths(conf, getPath().toString());
    conf.setInputFormat(ColumnFamilyInputFormat.class);

    /**
    // I missed you, java. Stay classy.
    Iterator<Map.Entry<String,String>> iter = conf.iterator();
    while (iter.hasNext()) {
      Map.Entry entry = iter.next();
      System.out.println(entry.getKey() + " : "  + entry.getValue());
    }
    */
  }

  public Path getPath() {
    return new Path(pathUUID);
  }

  public String getIdentifier() {
    return host + "_" + port + "_" + keyspace + "_" + columnFamily;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other)
      return true;
    if (!(other instanceof CassandraScheme))
      return false;
    if (!super.equals(other))
      return false;

    CassandraScheme that = (CassandraScheme) other;

    if (!getPath().toString().equals(that.getPath().toString()))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + getPath().toString().hashCode();
    result = 31 * result + (host != null ? host.hashCode() : 0);
    result = 31 * result + (port != null ? port.hashCode() : 0);
    result = 31 * result + (keyspace != null ? keyspace.hashCode() : 0);
    result = 31 * result + (columnFamily != null ? columnFamily.hashCode() : 0);
    return result;
  }


  private List<String> getSourceColumns() {
    if (this.settings.containsKey("source.columns")) {
      return (List<String>) this.settings.get("source.columns");
    } else {
      return new ArrayList<String>();
    }
  }

  private Map<String, String> getSourceTypes() {
    if (this.settings.containsKey("source.types")) {
      return (Map<String, String>) this.settings.get("source.types");
    } else {
      return new HashMap<String, String>();
    }
  }

  private ISink getSinkImpl(String className)
  {
      try {
          if (className==null) {
              return new StaticRowSink();
          }
          else {
              Class<ISink> klass = (Class<ISink>)Class.forName(className);
              return klass.newInstance();
          }
      } catch (InstantiationException e) {
          throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
      } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
      }
  }
}
