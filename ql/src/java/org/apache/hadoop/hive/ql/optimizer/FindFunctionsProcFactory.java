/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hive.ql.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.ConstantPropagateProcCtx.ConstantPropagateOption;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCase;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFNvl;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFStruct;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToUnixTimeStamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUnixTimeStamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFWhen;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardConstantStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

/**
 * Factory for generating the different node processors used by FindFunctions.
 */
public final class FindFunctionsProcFactory {
  protected static final Logger LOG = LoggerFactory.getLogger(FindFunctionsProcFactory.class.getName());

  private FindFunctionsProcFactory() {
    // prevent instantiation
  }


  /**
   * Node Processor for finding functions in Filter Operators. 
   */
  public static class FindFUnctionsFilterProc implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs)
        throws SemanticException {
      FindFunctionsCtx fctx = (FindFunctionsCtx)ctx;
      FilterOperator op = (FilterOperator) nd;

      ExprNodeDesc condn = op.getConf().getPredicate();   
      fctx.addFunctions(ExprNodeDescUtils.getGenericFuncDescs(condn));
      
      return null;
    }

  }

  /**
   * Factory method to get the ConstantPropagateFilterProc class.
   *
   * @return ConstantPropagateFilterProc
   */
  public static ConstantPropagateFilterProc getFilterProc() {
    return new ConstantPropagateFilterProc();
  }

  /**
   * Node Processor for Constant Propagate for Group By Operators.
   */
  public static class ConstantPropagateGroupByProc implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs)
        throws SemanticException {
      GroupByOperator op = (GroupByOperator) nd;
      ConstantPropagateProcCtx cppCtx = (ConstantPropagateProcCtx) ctx;
      Map<ColumnInfo, ExprNodeDesc> colToConstants = cppCtx.getPropagatedConstants(op);
      cppCtx.getOpToConstantExprs().put(op, colToConstants);

      RowSchema rs = op.getSchema();
      if (op.getColumnExprMap() != null && rs != null) {
        for (ColumnInfo colInfo : rs.getSignature()) {
          if (!VirtualColumn.isVirtualColumnBasedOnAlias(colInfo)) {
            ExprNodeDesc expr = op.getColumnExprMap().get(colInfo.getInternalName());
            if (expr instanceof ExprNodeConstantDesc) {
              colToConstants.put(colInfo, expr);
            }
          }
        }
      }

      if (colToConstants.isEmpty()) {
        return null;
      }

      GroupByDesc conf = op.getConf();
      ArrayList<ExprNodeDesc> keys = conf.getKeys();
      for (int i = 0; i < keys.size(); i++) {
        ExprNodeDesc key = keys.get(i);
        ExprNodeDesc newkey = foldExpr(key, colToConstants, cppCtx, op, 0, false);
        keys.set(i, newkey);
      }
      foldOperator(op, cppCtx);
      return null;
    }
  }

  /**
   * Factory method to get the ConstantPropagateGroupByProc class.
   *
   * @return ConstantPropagateGroupByProc
   */
  public static ConstantPropagateGroupByProc getGroupByProc() {
    return new ConstantPropagateGroupByProc();
  }

  /**
   * The Default Node Processor for Constant Propagation.
   */
  public static class ConstantPropagateDefaultProc implements NodeProcessor {
    @Override
    @SuppressWarnings("unchecked")
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs)
        throws SemanticException {
      ConstantPropagateProcCtx cppCtx = (ConstantPropagateProcCtx) ctx;
      Operator<? extends Serializable> op = (Operator<? extends Serializable>) nd;
      Map<ColumnInfo, ExprNodeDesc> constants = cppCtx.getPropagatedConstants(op);
      cppCtx.getOpToConstantExprs().put(op, constants);
      RowSchema rs = op.getSchema();
      if (op.getColumnExprMap() != null && rs != null) {
        for (ColumnInfo colInfo : rs.getSignature()) {
          if (!VirtualColumn.isVirtualColumnBasedOnAlias(colInfo)) {
            ExprNodeDesc expr = op.getColumnExprMap().get(colInfo.getInternalName());
            if (expr instanceof ExprNodeConstantDesc) {
              constants.put(colInfo, expr);
            }
          }
        }
      }
      if (constants.isEmpty()) {
        return null;
      }
      foldOperator(op, cppCtx);
      return null;
    }
  }

  /**
   * Factory method to get the ConstantPropagateDefaultProc class.
   *
   * @return ConstantPropagateDefaultProc
   */
  public static ConstantPropagateDefaultProc getDefaultProc() {
    return new ConstantPropagateDefaultProc();
  }

  /**
   * The Node Processor for Constant Propagation for Select Operators.
   */
  public static class ConstantPropagateSelectProc implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs)
        throws SemanticException {
      SelectOperator op = (SelectOperator) nd;
      ConstantPropagateProcCtx cppCtx = (ConstantPropagateProcCtx) ctx;
      Map<ColumnInfo, ExprNodeDesc> constants = cppCtx.getPropagatedConstants(op);
      cppCtx.getOpToConstantExprs().put(op, constants);
      foldOperator(op, cppCtx);
      List<ExprNodeDesc> colList = op.getConf().getColList();
      List<String> columnNames = op.getConf().getOutputColumnNames();
      Map<String, ExprNodeDesc> columnExprMap = op.getColumnExprMap();
      if (colList != null) {
        for (int i = 0; i < colList.size(); i++) {
          ExprNodeDesc newCol = foldExpr(colList.get(i), constants, cppCtx, op, 0, false);
          if (!(colList.get(i) instanceof ExprNodeConstantDesc) && newCol instanceof ExprNodeConstantDesc) {
            // Lets try to store original column name, if this column got folded
            // This is useful for optimizations like GroupByOptimizer
            String colName = colList.get(i).getExprString();
            if (HiveConf.getPositionFromInternalName(colName) == -1) {
              // if its not an internal name, this is what we want.
              ((ExprNodeConstantDesc)newCol).setFoldedFromCol(colName);
            } else {
              // If it was internal column, lets try to get name from columnExprMap
              ExprNodeDesc desc = columnExprMap.get(colName);
              if (desc instanceof ExprNodeConstantDesc) {
                ((ExprNodeConstantDesc)newCol).setFoldedFromCol(((ExprNodeConstantDesc)desc).getFoldedFromCol());
              }
            }
          }
          colList.set(i, newCol);
          if (newCol instanceof ExprNodeConstantDesc && op.getSchema() != null) {
            ColumnInfo colInfo = op.getSchema().getSignature().get(i);
            if (!VirtualColumn.isVirtualColumnBasedOnAlias(colInfo)) {
              constants.put(colInfo, newCol);
            }
          }
          if (columnExprMap != null) {
            columnExprMap.put(columnNames.get(i), newCol);
          }
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("New column list:(" + StringUtils.join(colList, " ") + ")");
        }
      }
      return null;
    }
  }

  /**
   * The Factory method to get the ConstantPropagateSelectProc class.
   *
   * @return ConstantPropagateSelectProc
   */
  public static ConstantPropagateSelectProc getSelectProc() {
    return new ConstantPropagateSelectProc();
  }

  /**
   * The Node Processor for constant propagation for FileSink Operators. In addition to constant
   * propagation, this processor also prunes dynamic partitions to static partitions if possible.
   */
  public static class ConstantPropagateFileSinkProc implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs)
        throws SemanticException {
      FileSinkOperator op = (FileSinkOperator) nd;
      ConstantPropagateProcCtx cppCtx = (ConstantPropagateProcCtx) ctx;
      Map<ColumnInfo, ExprNodeDesc> constants = cppCtx.getPropagatedConstants(op);
      cppCtx.getOpToConstantExprs().put(op, constants);
      if (constants.isEmpty()) {
        return null;
      }
      FileSinkDesc fsdesc = op.getConf();
      DynamicPartitionCtx dpCtx = fsdesc.getDynPartCtx();
      if (dpCtx != null) {

        // Assume only 1 parent for FS operator
        Operator<? extends Serializable> parent = op.getParentOperators().get(0);
        Map<ColumnInfo, ExprNodeDesc> parentConstants = cppCtx.getPropagatedConstants(parent);
        RowSchema rs = parent.getSchema();
        boolean allConstant = true;
        int dpColStartIdx = Utilities.getDPColOffset(fsdesc);
        List<ColumnInfo> colInfos = rs.getSignature();
        for (int i = dpColStartIdx; i < colInfos.size(); i++) {
          ColumnInfo ci = colInfos.get(i);
          if (parentConstants.get(ci) == null) {
            allConstant = false;
            break;
          }
        }
        if (allConstant) {
          pruneDP(fsdesc);
        }
      }
      foldOperator(op, cppCtx);
      return null;
    }

    private void pruneDP(FileSinkDesc fsdesc) {
      // FIXME: Support pruning dynamic partitioning.
      LOG.info("DP can be rewritten to SP!");
    }
  }

  public static NodeProcessor getFileSinkProc() {
    return new ConstantPropagateFileSinkProc();
  }

  /**
   * The Node Processor for Constant Propagation for Operators which is designed to stop propagate.
   * Currently these kinds of Operators include UnionOperator and ScriptOperator.
   */
  public static class ConstantPropagateStopProc implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs)
        throws SemanticException {
      Operator<?> op = (Operator<?>) nd;
      ConstantPropagateProcCtx cppCtx = (ConstantPropagateProcCtx) ctx;
      cppCtx.getOpToConstantExprs().put(op, new HashMap<ColumnInfo, ExprNodeDesc>());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Stop propagate constants on op " + op.getOperatorId());
      }
      return null;
    }
  }

  public static NodeProcessor getStopProc() {
    return new ConstantPropagateStopProc();
  }

  /**
   * The Node Processor for Constant Propagation for ReduceSink Operators. If the RS Operator is for
   * a join, then only those constants from inner join tables, or from the 'inner side' of a outer
   * join (left table for left outer join and vice versa) can be propagated.
   */
  public static class ConstantPropagateReduceSinkProc implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs)
        throws SemanticException {
      ReduceSinkOperator op = (ReduceSinkOperator) nd;
      ReduceSinkDesc rsDesc = op.getConf();
      ConstantPropagateProcCtx cppCtx = (ConstantPropagateProcCtx) ctx;
      Map<ColumnInfo, ExprNodeDesc> constants = cppCtx.getPropagatedConstants(op);

      cppCtx.getOpToConstantExprs().put(op, constants);
      RowSchema rs = op.getSchema();
      if (op.getColumnExprMap() != null && rs != null) {
        for (ColumnInfo colInfo : rs.getSignature()) {
          if (!VirtualColumn.isVirtualColumnBasedOnAlias(colInfo)) {
            ExprNodeDesc expr = op.getColumnExprMap().get(colInfo.getInternalName());
            if (expr instanceof ExprNodeConstantDesc) {
              constants.put(colInfo, expr);
            }
          }
        }
      }
      if (constants.isEmpty()) {
        return null;
      }

      if (op.getChildOperators().size() == 1
          && op.getChildOperators().get(0) instanceof JoinOperator) {
        JoinOperator joinOp = (JoinOperator) op.getChildOperators().get(0);
        if (skipFolding(joinOp.getConf())) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Skip folding in outer join " + op);
          }
          cppCtx.getOpToConstantExprs().put(op, new HashMap<ColumnInfo, ExprNodeDesc>());
          return null;
        }
      }

      if (rsDesc.getDistinctColumnIndices() != null
          && !rsDesc.getDistinctColumnIndices().isEmpty()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skip folding in distinct subqueries " + op);
        }
        cppCtx.getOpToConstantExprs().put(op, new HashMap<ColumnInfo, ExprNodeDesc>());
        return null;
      }

      // key columns
      ArrayList<ExprNodeDesc> newKeyEpxrs = new ArrayList<ExprNodeDesc>();
      for (ExprNodeDesc desc : rsDesc.getKeyCols()) {
        ExprNodeDesc newDesc = foldExpr(desc, constants, cppCtx, op, 0, false);
        if (newDesc != desc && desc instanceof ExprNodeColumnDesc && newDesc instanceof ExprNodeConstantDesc) {
          ((ExprNodeConstantDesc)newDesc).setFoldedFromCol(((ExprNodeColumnDesc)desc).getColumn());
        }
        newKeyEpxrs.add(newDesc);
      }
      rsDesc.setKeyCols(newKeyEpxrs);

      // partition columns
      ArrayList<ExprNodeDesc> newPartExprs = new ArrayList<ExprNodeDesc>();
      for (ExprNodeDesc desc : rsDesc.getPartitionCols()) {
        ExprNodeDesc expr = foldExpr(desc, constants, cppCtx, op, 0, false);
        if (expr != desc && desc instanceof ExprNodeColumnDesc
            && expr instanceof ExprNodeConstantDesc) {
          ((ExprNodeConstantDesc) expr).setFoldedFromCol(((ExprNodeColumnDesc) desc).getColumn());
        }
        newPartExprs.add(expr);
      }
      rsDesc.setPartitionCols(newPartExprs);

      // value columns
      ArrayList<ExprNodeDesc> newValExprs = new ArrayList<ExprNodeDesc>();
      for (ExprNodeDesc desc : rsDesc.getValueCols()) {
        newValExprs.add(foldExpr(desc, constants, cppCtx, op, 0, false));
      }
      rsDesc.setValueCols(newValExprs);
      foldOperator(op, cppCtx);
      return null;
    }

    /**
     * Skip folding constants if there is outer join in join tree.
     * @param joinDesc
     * @return true if to skip.
     */
    private boolean skipFolding(JoinDesc joinDesc) {
      for (JoinCondDesc cond : joinDesc.getConds()) {
        if (cond.getType() == JoinDesc.INNER_JOIN || cond.getType() == JoinDesc.UNIQUE_JOIN
            || cond.getType() == JoinDesc.LEFT_SEMI_JOIN) {
          continue;
        }
        return true;
      }
      return false;
    }

  }

  public static NodeProcessor getReduceSinkProc() {
    return new ConstantPropagateReduceSinkProc();
  }

  /**
   * The Node Processor for Constant Propagation for Join Operators.
   */
  public static class ConstantPropagateJoinProc implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs)
        throws SemanticException {
      JoinOperator op = (JoinOperator) nd;
      JoinDesc conf = op.getConf();
      ConstantPropagateProcCtx cppCtx = (ConstantPropagateProcCtx) ctx;
      Map<ColumnInfo, ExprNodeDesc> constants = cppCtx.getPropagatedConstants(op);
      cppCtx.getOpToConstantExprs().put(op, constants);
      if (constants.isEmpty()) {
        return null;
      }

      // Note: the following code (removing folded constants in exprs) is deeply coupled with
      //    ColumnPruner optimizer.
      // Assuming ColumnPrunner will remove constant columns so we don't deal with output columns.
      //    Except one case that the join operator is followed by a redistribution (RS operator).
      if (op.getChildOperators().size() == 1
          && op.getChildOperators().get(0) instanceof ReduceSinkOperator) {
        LOG.debug("Skip JOIN-RS structure.");
        return null;
      }
      if (LOG.isInfoEnabled()) {
        LOG.info("Old exprs " + conf.getExprs());
      }
      Iterator<Entry<Byte, List<ExprNodeDesc>>> itr = conf.getExprs().entrySet().iterator();
      while (itr.hasNext()) {
        Entry<Byte, List<ExprNodeDesc>> e = itr.next();
        int tag = e.getKey();
        List<ExprNodeDesc> exprs = e.getValue();
        if (exprs == null) {
          continue;
        }
        List<ExprNodeDesc> newExprs = new ArrayList<ExprNodeDesc>();
        for (ExprNodeDesc expr : exprs) {
          ExprNodeDesc newExpr = foldExpr(expr, constants, cppCtx, op, tag, false);
          if (newExpr instanceof ExprNodeConstantDesc) {
            if (LOG.isInfoEnabled()) {
              LOG.info("expr " + newExpr + " fold from " + expr + " is removed.");
            }
            continue;
          }
          newExprs.add(newExpr);
        }
        e.setValue(newExprs);
      }
      if (LOG.isInfoEnabled()) {
        LOG.info("New exprs " + conf.getExprs());
      }

      for (List<ExprNodeDesc> v : conf.getFilters().values()) {
        for (int i = 0; i < v.size(); i++) {
          ExprNodeDesc expr = foldExpr(v.get(i), constants, cppCtx, op, 0, false);
          v.set(i, expr);
        }
      }
      foldOperator(op, cppCtx);
      return null;
    }

  }

  public static NodeProcessor getJoinProc() {
    return new ConstantPropagateJoinProc();
  }

  /**
   * The Node Processor for Constant Propagation for Table Scan Operators.
   */
  public static class ConstantPropagateTableScanProc implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs)
        throws SemanticException {
      TableScanOperator op = (TableScanOperator) nd;
      TableScanDesc conf = op.getConf();
      ConstantPropagateProcCtx cppCtx = (ConstantPropagateProcCtx) ctx;
      Map<ColumnInfo, ExprNodeDesc> constants = cppCtx.getPropagatedConstants(op);
      cppCtx.getOpToConstantExprs().put(op, constants);
      ExprNodeGenericFuncDesc pred = conf.getFilterExpr();
      if (pred == null) {
        return null;
      }

      ExprNodeDesc constant = foldExpr(pred, constants, cppCtx, op, 0, false);
      if (constant instanceof ExprNodeGenericFuncDesc) {
        conf.setFilterExpr((ExprNodeGenericFuncDesc) constant);
      } else {
        conf.setFilterExpr(null);
      }
      return null;
    }
  }

  public static NodeProcessor getTableScanProc() {
    return new ConstantPropagateTableScanProc();
  }
}
