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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.LeafNode;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.TreeNode;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.TreeVisitor;

import com.google.common.collect.ImmutableList;


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

  public static abstract class FilterPlan {
    abstract FilterPlan and(FilterPlan other);
    abstract FilterPlan or(FilterPlan other);
    abstract List<ScanPlan> getPlans();
    @Override
    public String toString() {
      return getPlans().toString();
    }

  }

  /**
   * Represents a union/OR of single scan plans (ScanPlan).
   */
  public static class MultiScanPlan extends FilterPlan {
    final ImmutableList<ScanPlan> scanPlans;

    public MultiScanPlan(List<ScanPlan> scanPlans){
      this.scanPlans = ImmutableList.copyOf(scanPlans);
    }

    @Override
    public FilterPlan and(FilterPlan other) {
      // Convert to disjunctive normal form (DNF), ie OR of ANDs
      // First get a new set of FilterPlans by doing an AND
      // on each ScanPlan in this one with the other FilterPlan
      List<FilterPlan> newFPlans = new ArrayList<FilterPlan>();
      for (ScanPlan splan : getPlans()) {
        newFPlans.add(splan.and(other));
      }
      //now combine scanPlans in multiple new FilterPlans into one
      // MultiScanPlan
      List<ScanPlan> newScanPlans = new ArrayList<ScanPlan>();
      for (FilterPlan fp : newFPlans) {
        newScanPlans.addAll(fp.getPlans());
      }
      return new MultiScanPlan(newScanPlans);
    }

    @Override
    public FilterPlan or(FilterPlan other) {
      // just combine the ScanPlans
      List<ScanPlan> newScanPlans = new ArrayList<ScanPlan>(this.getPlans());
      newScanPlans.addAll(other.getPlans());
      return new MultiScanPlan(newScanPlans);
    }

    @Override
    public List<ScanPlan> getPlans() {
      return scanPlans;
    }
  }

  public static class ScanPlan extends FilterPlan {

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

    @Override
    public FilterPlan and(FilterPlan other) {
      List<ScanPlan> newSPlans = new ArrayList<ScanPlan>();
      for(ScanPlan otherSPlan : other.getPlans()) {
        newSPlans.add(this.and(otherSPlan));
      }
      return new MultiScanPlan(newSPlans);
    }

    private ScanPlan and(ScanPlan other) {
      // create combined FilterPlan based on existing lhs and rhs plan
      ScanPlan newPlan = new ScanPlan();

      // create new scan start
      ScanMarker greaterStartMarker = getComparedMarker(this.getStartMarker(),
          other.getStartMarker(), true);
      newPlan.setStartMarker(greaterStartMarker);

      // create new scan end
      ScanMarker lesserEndMarker = getComparedMarker(this.getEndMarker(), other.getEndMarker(),
          false);
      newPlan.setEndMarker(lesserEndMarker);

      // create new filter plan
      newPlan.setFilter(createCombinedFilter(this.getFilter(), other.getFilter()));

      return newPlan;
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
    public FilterPlan or(FilterPlan other) {
      List<ScanPlan> plans = new ArrayList<ScanPlan>(getPlans());
      plans.addAll(other.getPlans());
      return new MultiScanPlan(plans);
    }

    @Override
    public List<ScanPlan> getPlans() {
      return Arrays.asList(this);
    }


    /**
     * @return row suffix - This is appended to db + table, to generate start row for the Scan
     */
    public byte[] getStartRowSuffix() {
      if (startMarker.isInclusive) {
        return startMarker.bytes;
      } else {
        // append a 0 byte to make it start-exclusive
        return ArrayUtils.add(startMarker.bytes, (byte) 0);
      }
    }

    /**
     * @return row suffix - This is appended to db + table, to generate end row for the Scan
     */
    public byte[] getEndRowSuffix() {
      if (endMarker.isInclusive) {
        // append a 0 byte to make end row inclusive
        return ArrayUtils.add(endMarker.bytes, (byte) 0);
      } else {
        return endMarker.bytes;
      }
    }

    @Override
    public String toString() {
      return "ScanPlan [startMarker=" + startMarker + ", endMarker=" + endMarker + ", filter="
          + filter + "]";
    }

  }

  /**
   * represent a plan that can be used to create a hbase filter and then set in
   * Scan.setFilter()
   */
  public static class ScanFilter {
    // TODO: implement this
  }

  private static class PartitionFilterGenerator extends TreeVisitor {
    FilterPlan curPlan;

    // temporary params for current left and right side plans, for AND, OR
    FilterPlan lPlan, rPlan;

    FilterPlan getPlan() {
      return curPlan;
    }

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
      switch (node.getAndOr()) {
      case AND:
        curPlan = lPlan.and(rPlan);
        break;
      case OR:
        curPlan = lPlan.or(rPlan);
        break;
      default:
        throw new AssertionError("Unexpected logical operation " + node.getAndOr());
      }

    }


    @Override
    public void visit(LeafNode node) throws MetaException {
      ScanPlan leafPlan = new ScanPlan();

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

  public static FilterPlan getFilterPlan(ExpressionTree exprTree) throws MetaException {
    PartitionFilterGenerator pGenerator = new PartitionFilterGenerator();
    exprTree.accept(pGenerator);
    return pGenerator.getPlan();
  }

}
