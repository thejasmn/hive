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
import org.apache.hadoop.hive.metastore.hbase.HBaseFilterPlanUtil.FilterPlan.ScanMarker;
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

  /**
   * Compare two byte arrays
   *
   * @param ar1
   *          first byte array
   * @param ar2
   *          second byte array
   * @return -1 if ar1 < ar2, 0 if == , 1 if >
   */
  static int compare(byte[] ar1, byte[] ar2) {
    if (ar1 == null) {
      if (ar2 == null) {
        return 0;
      }
      return -1;
    }
    if (ar2 == null) {
      return 1;
    }

    for (int i = 0; i < ar1.length; i++) {
      if (i > ar2.length) {
        return 1;
      } else {
        if (ar1[i] == ar2[i]) {
          continue;
        } else if (ar1[i] > ar2[i]) {
          return 1;
        } else {
          return -1;
        }
      }
    }
    // ar2 equal until length of ar1.
    if(ar1.length == ar2.length) {
      return 0;
    }
    // ar2 has more bytes
    return -1;
  }

  public static class FilterPlan {

    public static class ScanMarker {
      final byte[] bytes;
      final boolean isInclusive;
      ScanMarker(byte [] b, boolean i){
        this.bytes = b;
        this.isInclusive = i;
      }
    }
    // represent Scan start
    private ScanMarker startMarker = new ScanMarker(null, false);
    // represent Scan end
    private ScanMarker endMarker = new ScanMarker(null, false);

    private ScanFilter filter;


    public ScanFilter getFilter() {
      return filter;
    }

    public void setFilter(ScanFilter filter) {
      this.filter = filter;
    }

    public ScanMarker getStartMarker() {
      return startMarker;
    }

    public void setStartMarker(ScanMarker startMarker) {
      this.startMarker = startMarker;
    }
    public void setStartMarker(byte[] start, boolean isInclusive) {
      setStartMarker(new ScanMarker(start, isInclusive));
    }

    public ScanMarker getEndMarker() {
      return endMarker;
    }

    public void setEndMarker(ScanMarker endMarker) {
      this.endMarker = endMarker;
    }
    public void setEndMarker(byte[] end, boolean isInclusive) {
      setEndMarker(new ScanMarker(end, isInclusive));
    }
  }

  public static class ScanFilter {

  }

  private static class PartitionFilterGenerator extends TreeVisitor {
    FilterPlan curPlan;

    // temporary params for current left and right side plans, for AND, OR
    FilterPlan lPlan, rPlan;

    @Override
    protected void beginTreeNode(TreeNode node) throws MetaException {
      curPlan = lPlan = rPlan = null;
    }

    @Override
    protected void midTreeNode(TreeNode node) throws MetaException {
      lPlan = curPlan;
      curPlan = null;
    }

    @Override
    protected void endTreeNode(TreeNode node) throws MetaException {
      rPlan = curPlan;
      curPlan = new FilterPlan();
      switch (node.getAndOr()) {
      case AND:
        // create combined FilterPlan based on existing lhs and rhs plan
        // create new scan start
        ScanMarker greaterStartMarker = getComparedMarker(lPlan.getStartMarker(),
            rPlan.getStartMarker(), true);
        curPlan.setStartMarker(greaterStartMarker);

        // create new scan end
        ScanMarker lesserEndMarker = getComparedMarker(lPlan.getEndMarker(), rPlan.getEndMarker(),
            false);
        curPlan.setEndMarker(lesserEndMarker);

        curPlan.setFilter(createCombinedFilter(lPlan.getFilter(), rPlan.getFilter()));

        break;
      case OR:


        break;
      default:
        throw new AssertionError("Unexpected logical operation " + node.getAndOr());
      }

    }

    private ScanFilter createCombinedFilter(ScanFilter filter1, ScanFilter filter2) {
      // TODO create combined filter - filter1 && filter2
      return null;
    }

    private ScanMarker getComparedMarker(ScanMarker lStartMarker, ScanMarker rStartMarker,
        boolean getGreater) {
      int compareRes = compare(lStartMarker.bytes, rStartMarker.bytes);
      if (compareRes == 0) {
        // bytes are equal, now compare the isInclusive flags
        if (lStartMarker.isInclusive == rStartMarker.isInclusive) {
          // actually equal, so return any one
          return lStartMarker;
        }
        boolean isInclusive = true;
        // one that does not include the current bytes is greater
        if (getGreater) {
          isInclusive = false;
        }
        // else
        return new ScanMarker(lStartMarker.bytes, isInclusive);
      }
      if (getGreater) {
        return compareRes == 1 ? lStartMarker : rStartMarker;
      }
      // else
      return compareRes == -1 ? lStartMarker : rStartMarker;
    }

    @Override
    public void visit(LeafNode node) throws MetaException {
      FilterPlan leafPlan = new FilterPlan();

      if (!isFirstParitionColumn(node.keyName)) {
        leafPlan.setFilter(generateScanFilter(node));
        curPlan = leafPlan;
        return;
      }

      // this is a condition on first partition column, so might influence the
      // start and end of the scan
      final boolean INCLUSIVE = true;
      switch (node.operator) {
      case EQUALS:
        leafPlan.setStartMarker(toBytes(node.value), INCLUSIVE);
        leafPlan.setEndMarker(toBytes(node.value), INCLUSIVE);
        break;
      case GREATERTHAN:
        leafPlan.setStartMarker(toBytes(node.value), INCLUSIVE);
        break;
      case GREATERTHANOREQUALTO:
        leafPlan.setStartMarker(toBytes(node.value), INCLUSIVE);
        break;
      case LESSTHAN:
        leafPlan.setEndMarker(toBytes(node.value), INCLUSIVE);
        break;
      case LESSTHANOREQUALTO:
        leafPlan.setEndMarker(toBytes(node.value), INCLUSIVE);
        break;
      case LIKE:
      case NOTEQUALS:
      case NOTEQUALS2:
        // TODO: create filter plan for these
        break;
      }
    }

    private byte[] toBytes(Object value) {
      // TODO: actually implement this
      // We need to determine the actual type and use appropriate
      // serialization format for that type
      return ((String) value).getBytes(HBaseUtils.ENCODING);
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
