/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.thrift;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SaslRpcServer;

/**
 * Functions that bridge Thrift's SASL transports to Hadoop's SASL callback
 * handlers and authentication classes.
 *
 * This is a 0.23/2.x specific implementation
 */
public class HadoopThriftAuthBridge23 extends HadoopThriftAuthBridge20S {

  static Field SASL_PROPS_FIELD;
  static Class SASL_PROPERTIES_RESOLVER_CLASS;
  static {
    SASL_PROPERTIES_RESOLVER_CLASS = null;
    final String SASL_PROP_RES_CLASSNAME = "org.apache.hadoop.security.SaslPropertiesResolver";
    try {
      SASL_PROPERTIES_RESOLVER_CLASS = Class.forName(SASL_PROP_RES_CLASSNAME);
      // found the class, so this would be hadoop version 2.4 or newer (See
      // HADOOP-10221, HADOOP-10451)
    } catch (ClassNotFoundException e) {
    }

    if (SASL_PROPERTIES_RESOLVER_CLASS == null) {
      // this must be a pre hadoop 2.4 version
      try {
        SASL_PROPS_FIELD = SaslRpcServer.class.getField("SASL_PROPS");
      } catch (NoSuchFieldException e) {
        // Older version of hadoop should have had this field
        throw new IllegalStateException("Error finding hadoop SASL properties resolver class", e);
      }
    }
  }

  /**
   * Read and return Hadoop SASL configuration which can be configured using
   * "hadoop.rpc.protection"
   *
   * @param conf
   * @return Hadoop SASL configuration
   */
  @SuppressWarnings("unchecked")
  @Override
  public Map<String, String> getHadoopSaslProperties(Configuration conf) {
    if (SASL_PROPS_FIELD != null) {
      // pre hadoop 2.4 way of finding the sasl property settings
      // Initialize the SaslRpcServer to ensure QOP parameters are read from
      // conf
      SaslRpcServer.init(conf);
      try {
        return (Map<String, String>) SASL_PROPS_FIELD.get(null);
      } catch (Exception e) {
        throw new IllegalStateException("Error finding hadoop SASL properties", e);
      }
    }
    // 2.4 and later way of finding sasl property
    try {
      Method getInstanceMethod = SASL_PROPERTIES_RESOLVER_CLASS.getMethod("getInstance",
          Configuration.class);
      Method getDefaultPropertiesMethod = SASL_PROPERTIES_RESOLVER_CLASS.getMethod(
          "getDefaultProperties");
      Configurable saslPropertiesResolver = (Configurable) getInstanceMethod.invoke(null, conf);
      saslPropertiesResolver.setConf(conf);
      return (Map<String, String>) getDefaultPropertiesMethod.invoke(saslPropertiesResolver);
    } catch (Exception e) {
      throw new IllegalStateException("Error finding hadoop SASL properties", e);
    }
  }

}
