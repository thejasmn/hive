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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore.hbase;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.LeafNode;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.TreeNode;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.TreeVisitor;


/**
 * Utility function for generating hbase partition filtering plan representation
 * from ExpressionTree.
 * Correctness issues to be addressed
 *  - handle case where table has more than one partition column (assumes single partition column
 *  in this iteration)
 * Optimizations to be done -
 *  - Case where all partition keys are specified. Should use a get
 *
 */
class HBaseFilterPlanUtil {
  public static class FilterPlan {

    // represent Scan start
    private byte[] start;
    private boolean isStartInclusive = false;
    // represent Scan end
    private byte[] end;
    private boolean isEndInclusive = false;

    private ScanFilter filter;

    public byte[] getStart() {
      return start;
    }

    public void setStart(byte[] start) {
      this.start = start;
    }

    public boolean isStartInclusive() {
      return isStartInclusive;
    }

    public void setStartInclusive(boolean isStartInclusive) {
      this.isStartInclusive = isStartInclusive;
    }

    public byte[] getEnd() {
      return end;
    }

    public void setEnd(byte[] end) {
      this.end = end;
    }

    public boolean isEndInclusive() {
      return isEndInclusive;
    }

    public void setEndInclusive(boolean isEndInclusive) {
      this.isEndInclusive = isEndInclusive;
    }

    public ScanFilter getFilter() {
      return filter;
    }

    public void setFilter(ScanFilter filter) {
      this.filter = filter;
    }

  }
  public static class ScanFilter {

  }

  private static class PartitionFilterGenerator extends TreeVisitor {
    FilterPlan curPlan;

    // temporary params for current left and right side plans, for AND, OR
    FilterPlan tmpLPlan, tmpRPlan;


    @Override
    protected void beginTreeNode(TreeNode node) throws MetaException {
      curPlan = tmpLPlan = tmpRPlan = null;
    }

    @Override
    protected void midTreeNode(TreeNode node) throws MetaException {
      tmpLPlan = curPlan;
      curPlan = null;
    }

    @Override
    protected void endTreeNode(TreeNode node) throws MetaException {
      tmpRPlan = curPlan;
      //TODO: generate current plan
    }

    @Override
    public void visit(LeafNode node) throws MetaException {
      FilterPlan leafPlan = new FilterPlan();

      if(!isFirstParitionColumn(node.keyName)) {
        leafPlan.setFilter(generateScanFilter(node));
        curPlan = leafPlan;
        return;
      }

      // this is a condition on first partition column, so might influence the
      // start and end of the scan
      switch(node.operator) {
      case EQUALS:
        leafPlan.setStart(toBytes(node.value));

      }
    }

    private byte[] toBytes(Object value) {
      // TODO: actually implementt this
      return new byte[0];
    }

    private ScanFilter generateScanFilter(LeafNode node) {
      // TODO Auto-generated method stub
      return null;
    }

    private boolean isFirstParitionColumn(String keyName) {
      // TODO: actually do the check!
      return true;
    }


  }


  public static FilterPlan getFilterPlan(ExpressionTree exprTree) {


    // TODO Auto-generated method stub
    return null;
  }

}
