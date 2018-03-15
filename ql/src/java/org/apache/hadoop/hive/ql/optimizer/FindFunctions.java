/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.optimizer.ConstantPropagateProcCtx.ConstantPropagateOption;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Find functions used in the plan
 */
public class FindFunctions extends Transform {

  private static final Logger LOG = LoggerFactory.getLogger(FindFunctions.class);
  protected ParseContext pGraphContext;
  
  public FindFunctions() {
  }

  /**
   * Transform the query tree.
   *
   * @param pactx
   *        the current parse context
   */
  @Override
  public ParseContext transform(ParseContext pactx) throws SemanticException {
    pGraphContext = pactx;
    FindFunctionsCtx fctx = new FindFunctionsCtx();
    
    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack. The dispatcher
    // generates the plan from the operator tree
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

    opRules.put(new RuleRegExp("R1", FilterOperator.getOperatorName() + "%"),
        ConstantPropagateProcFactory.getFilterProc());
    opRules.put(new RuleRegExp("R2", GroupByOperator.getOperatorName() + "%"),
        ConstantPropagateProcFactory.getGroupByProc());
    opRules.put(new RuleRegExp("R3", SelectOperator.getOperatorName() + "%"),
        ConstantPropagateProcFactory.getSelectProc());
    opRules.put(new RuleRegExp("R4", FileSinkOperator.getOperatorName() + "%"),
        ConstantPropagateProcFactory.getFileSinkProc());
    opRules.put(new RuleRegExp("R5", ReduceSinkOperator.getOperatorName() + "%"),
        ConstantPropagateProcFactory.getReduceSinkProc());
    opRules.put(new RuleRegExp("R6", JoinOperator.getOperatorName() + "%"),
        ConstantPropagateProcFactory.getJoinProc());
    opRules.put(new RuleRegExp("R7", TableScanOperator.getOperatorName() + "%"),
        ConstantPropagateProcFactory.getTableScanProc());
   

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(ConstantPropagateProcFactory
        .getDefaultProc(), opRules, fctx);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of operator nodes to start the walking.
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pGraphContext.getTopOps().values());
    ogw.startWalking(topNodes, null);
    System.out.println("Found functions " + fctx.getFunctions());
    System.out.println(fctx.getFunctions());
    return pGraphContext;
  }




}
