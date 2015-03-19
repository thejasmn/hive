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

import org.apache.hadoop.hive.metastore.hbase.HBaseFilterPlanUtil.ScanPlan;
import org.apache.hadoop.hive.metastore.hbase.HBaseFilterPlanUtil.ScanPlan.ScanMarker;
import org.junit.Assert;
import org.junit.Test;

public class TestHBaseFilterPlanUtil {
  final boolean INCLUSIVE = true;

  /**
   * Test the function that compares byte arrays
   */
  @Test
  public void testCompare() {

    Assert.assertEquals(-1, HBaseFilterPlanUtil.compare(new byte[]{1,2}, new byte[]{1,3}));
    Assert.assertEquals(-1, HBaseFilterPlanUtil.compare(new byte[]{1,2,3}, new byte[]{1,3}));
    Assert.assertEquals(-1, HBaseFilterPlanUtil.compare(new byte[]{1,2}, new byte[]{1,2,3}));

    Assert.assertEquals(0, HBaseFilterPlanUtil.compare(new byte[]{3,2}, new byte[]{3,2}));

    Assert.assertEquals(1, HBaseFilterPlanUtil.compare(new byte[]{3,2,1}, new byte[]{3,2}));
    Assert.assertEquals(1, HBaseFilterPlanUtil.compare(new byte[]{3,3,1}, new byte[]{3,2}));

  }


  /**
   * Test function that finds greater/lesser marker
   */
  @Test
  public void testgetComparedMarker() {
    ScanMarker l;
    ScanMarker r;

    // equal plans
    l = new ScanMarker(new byte[]{1,2}, INCLUSIVE);
    r = new ScanMarker(new byte[]{1,2}, INCLUSIVE);
    assertFirstGreater(l, r);

    l = new ScanMarker(new byte[]{1,2}, !INCLUSIVE);
    r = new ScanMarker(new byte[]{1,2}, !INCLUSIVE);
    assertFirstGreater(l, r);

    l = new ScanMarker(null, !INCLUSIVE);
    r = new ScanMarker(null, !INCLUSIVE);
    assertFirstGreater(l, r);

    // create l is greater because of inclusive flag
    l = new ScanMarker(new byte[]{1,2}, !INCLUSIVE);
    r = new ScanMarker(null, !INCLUSIVE);
    // the rule for null vs non-null is different
    // non-null is both smaller and greater than null
    Assert.assertEquals(l, ScanPlan.getComparedMarker(l, r, true));
    Assert.assertEquals(l, ScanPlan.getComparedMarker(r, l, true));
    Assert.assertEquals(l, ScanPlan.getComparedMarker(l, r, false));
    Assert.assertEquals(l, ScanPlan.getComparedMarker(r, l, false));


    // create l that is greater because of the bytes
    l = new ScanMarker(new byte[]{1,2,0}, INCLUSIVE);
    r = new ScanMarker(new byte[]{1,2}, INCLUSIVE);
    assertFirstGreater(l, r);

  }


  private void assertFirstGreater(ScanMarker big, ScanMarker small) {
    Assert.assertEquals(big, ScanPlan.getComparedMarker(big, small, true));
    Assert.assertEquals(big, ScanPlan.getComparedMarker(small, big, true));
    Assert.assertEquals(small, ScanPlan.getComparedMarker(big, small, false));
    Assert.assertEquals(small, ScanPlan.getComparedMarker(small, big, false));
  }


  /**
   * Test ScanPlan AND operation
   */
  @Test
  public void testScanPlanAnd() {
    ScanPlan l = new ScanPlan();
    ScanPlan r = new ScanPlan();
    l.setStartMarker(new ScanMarker(new byte[]{10}, INCLUSIVE));
    r.setStartMarker(new ScanMarker(new byte[]{10}, INCLUSIVE));

    ScanPlan res;
    // both equal
    res = l.and(r).getPlans().get(0);
    Assert.assertEquals(new ScanMarker(new byte[]{10}, INCLUSIVE), res.getStartMarker());

    // add equal end markers as well, and test AND again
    l.setEndMarker(new ScanMarker(new byte[]{20}, INCLUSIVE));
    r.setEndMarker(new ScanMarker(new byte[]{20}, INCLUSIVE));
    res = l.and(r).getPlans().get(0);
    Assert.assertEquals(new ScanMarker(new byte[]{10}, INCLUSIVE), res.getStartMarker());
    Assert.assertEquals(new ScanMarker(new byte[]{20}, INCLUSIVE), res.getEndMarker());


    l.setEndMarker(new ScanMarker(null, INCLUSIVE));
    r.setStartMarker(new ScanMarker(null, !INCLUSIVE));
    // markers with non null bytes are both lesser and greator
    Assert.assertEquals(l.getStartMarker(), res.getStartMarker());
    Assert.assertEquals(r.getEndMarker(), res.getEndMarker());

    l.setStartMarker(new ScanMarker(new byte[]{10,11}, !INCLUSIVE));
    l.setEndMarker(new ScanMarker(new byte[]{20,21}, INCLUSIVE));

    r.setStartMarker(new ScanMarker(new byte[]{10,10}, INCLUSIVE));
    r.setEndMarker(new ScanMarker(new byte[]{15}, INCLUSIVE));
    res = l.and(r).getPlans().get(0);
    // start of l is greater, end of r is smaller
    Assert.assertEquals(l.getStartMarker(), res.getStartMarker());
    Assert.assertEquals(r.getEndMarker(), res.getEndMarker());

  }


}
