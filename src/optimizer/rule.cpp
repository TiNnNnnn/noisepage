#include "optimizer/group_expression.h"
#include "optimizer/rules/implementation_rules.h"
#include "optimizer/rules/rewrite_rules.h"
#include "optimizer/rules/transformation_rules.h"
#include "optimizer/rules/wetune_rules.h"
#include "optimizer/rules/unnesting_rules.h"

#include <fstream>
#include <string>
#include <unordered_map>

namespace noisepage::optimizer {

RulePromise Rule::Promise(GroupExpression *group_expr) const {
  auto root_type = match_pattern_->Type();
  // This rule is not applicable
  if (root_type != OpType::LEAF && root_type != group_expr->Contents()->GetOpType()) {
    return RulePromise::NO_PROMISE;
  }
  if (IsPhysical()) return RulePromise::PHYSICAL_PROMISE;
  return RulePromise::LOGICAL_PROMISE;
}

RuleSet::RuleSet() {
  AddRule(RuleSetName::LOGICAL_TRANSFORMATION, new LogicalInnerJoinCommutativity());
  AddRule(RuleSetName::LOGICAL_TRANSFORMATION, new LogicalInnerJoinAssociativity());

  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalDeleteToPhysicalDelete());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalUpdateToPhysicalUpdate());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalInsertToPhysicalInsert());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalInsertSelectToPhysicalInsertSelect());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalGroupByToPhysicalHashGroupBy());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalAggregateToPhysicalAggregate());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalGetToPhysicalTableFreeScan());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalGetToPhysicalSeqScan());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalGetToPhysicalIndexScan());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalExternalFileGetToPhysicalExternalFileGet());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalQueryDerivedGetToPhysicalQueryDerivedScan());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalInnerJoinToPhysicalInnerIndexJoin());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalInnerJoinToPhysicalInnerNLJoin());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalSemiJoinToPhysicalSemiLeftHashJoin());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalInnerJoinToPhysicalInnerHashJoin());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalLeftJoinToPhysicalLeftHashJoin());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalLimitToPhysicalLimit());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalExportToPhysicalExport());

  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalCreateDatabaseToPhysicalCreateDatabase());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalCreateFunctionToPhysicalCreateFunction());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalCreateIndexToPhysicalCreateIndex());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalCreateTableToPhysicalCreateTable());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalCreateNamespaceToPhysicalCreateNamespace());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalCreateTriggerToPhysicalCreateTrigger());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalCreateViewToPhysicalCreateView());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalDropDatabaseToPhysicalDropDatabase());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalDropTableToPhysicalDropTable());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalDropIndexToPhysicalDropIndex());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalDropNamespaceToPhysicalDropNamespace());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalDropTriggerToPhysicalDropTrigger());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalDropViewToPhysicalDropView());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalAnalyzeToPhysicalAnalyze());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalCteScanToPhysicalCteScan());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalCteScanToPhysicalEmptyCteScan());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalCteScanToPhysicalCteScanIterative());

  AddRule(RuleSetName::PREDICATE_PUSH_DOWN, new RewritePushImplicitFilterThroughJoin());
  AddRule(RuleSetName::PREDICATE_PUSH_DOWN, new RewritePushExplicitFilterThroughJoin());
  AddRule(RuleSetName::PREDICATE_PUSH_DOWN, new RewritePushFilterThroughAggregation());
  AddRule(RuleSetName::PREDICATE_PUSH_DOWN, new RewriteCombineConsecutiveFilter());
  AddRule(RuleSetName::PREDICATE_PUSH_DOWN, new RewriteEmbedFilterIntoGet());
  AddRule(RuleSetName::PREDICATE_PUSH_DOWN, new RewriteEmbedFilterIntoCteScan());
  AddRule(RuleSetName::PREDICATE_PUSH_DOWN, new RewriteEmbedFilterIntoChildlessCteScan());

  AddRule(RuleSetName::UNNEST_SUBQUERY, new RewritePullFilterThroughMarkJoin());
  AddRule(RuleSetName::UNNEST_SUBQUERY, new UnnestMarkJoinToInnerJoin());
  AddRule(RuleSetName::UNNEST_SUBQUERY, new UnnestSingleJoinToInnerJoin());
  AddRule(RuleSetName::UNNEST_SUBQUERY, new DependentSingleJoinToInnerJoin());
  AddRule(RuleSetName::UNNEST_SUBQUERY, new RewritePullFilterThroughAggregation());
  AddRule(RuleSetName::PREDICATE_PUSH_DOWN, new RewriteUnionWithRecursiveCTE());

  //add logical wetune rules
  std::unordered_map<std::string,Rule*> wetune_rules;
  std::unordered_map<int,std::string>file_names;
  file_names[4] = "wetune_rules";
  read_wetune_rules(file_names,wetune_rules);
  for(const auto& r : wetune_rules){
    AddRule(RuleSetName::LOGICAL_WETUNE,r.second);
  }
}

void RuleSet::read_wetune_rules(std::unordered_map<int,std::string> &file_names,std::unordered_map<std::string,Rule*>&wetune_rules){
    
    for(auto fname : file_names){

      std::ifstream file(fname.second);
      //int rule_node_size = fname.first;

      ParseStage parse_stage;

      if (!file.is_open()) {
        std::cerr << "Failed to open file: " << fname.second << std::endl;
        return;
      }
      std::string rule;
      while (std::getline(file, rule)) {
        if (rule.empty()) continue;
        std::unique_ptr<ParsedSqlNode> sql_node;
        if(!parse_stage.handle_request(rule,sql_node)){
          std::cerr <<"Faild to parse rule file: "<< fname.second <<std::endl;
          return;
        }
        Rule* wetune_rule = new WeTuneRule(rule,std::move(sql_node));
        wetune_rules[rule] = wetune_rule;
      }
    }
}

/**
 * in now version,here no used,we just read wetune rule file directly
 */
void RuleSet::write_wetune_rules(std::string tb_name){}

}  // namespace noisepage::optimizer
