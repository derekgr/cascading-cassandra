package com.ifesdjeen.cascading.cassandra.hadoop;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.config.ConfigurationException;

import java.nio.ByteBuffer;

public class SerializerHelper {

  public static Object deserialize(ByteBuffer bb, String type) throws ConfigurationException {
    return inferType(type).compose(bb);
  }

  public static AbstractType inferType(String t) throws ConfigurationException {
    return org.apache.cassandra.db.marshal.TypeParser.parse(t);
  }
}
