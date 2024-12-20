#pragma once

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "catalog/schema.h"
#include "common/managed_pointer.h"
#include "parser/expression/abstract_expression.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "rule.h"

namespace noisepage::catalog {
class CatalogAccessor;
}

namespace noisepage::optimizer {

class AnnotatedExpression;

/**
 * Collection of utility functions for the optimizer
 */
class OptimizerUtil {
 public:
  /**
   * Check if a set is a subset of another set
   *
   * @param super_set The potential super set
   * @param child_set The potential child set
   *
   * @return True if the second set is a subset of the first one
   */
  template <class T>
  static bool IsSubset(const std::unordered_set<T> &super_set, const std::unordered_set<T> &child_set) {
    for (auto &element : child_set) {
      if (super_set.find(element) == super_set.end()) return false;
    }
    return true;
  }

  /**
   * Walks through a vector of join predicates. Generates join keys based on the sets of left
   * and right table aliases.
   *
   * @param join_predicates vector of join predicates
   * @param left_keys output vector of left keys
   * @param right_keys output vector of right keys
   * @param left_alias Alias set for left table
   * @param right_alias Alias set for right table
   */
  static void ExtractEquiJoinKeys(const std::vector<AnnotatedExpression> &join_predicates,
                                  std::vector<common::ManagedPointer<parser::AbstractExpression>> *left_keys,
                                  std::vector<common::ManagedPointer<parser::AbstractExpression>> *right_keys,
                                  const std::unordered_set<parser::AliasType> &left_alias,
                                  const std::unordered_set<parser::AliasType> &right_alias);

  /**
   * Generate all tuple value expressions of a base table
   *
   * @param accessor CatalogAccessor
   * @param alias Table alias used in constructing ColumnValue
   * @param db_oid Database OID
   * @param tbl_oid Table OID for catalog lookup
   * @return a vector of tuple value expression representing column name to
   * table column id mapping
   */
  static std::vector<parser::AbstractExpression *> GenerateTableColumnValueExprs(catalog::CatalogAccessor *accessor,
                                                                                 const parser::AliasType &alias,
                                                                                 catalog::db_oid_t db_oid,
                                                                                 catalog::table_oid_t tbl_oid);

  /**
   * Generate column value expression
   *
   * @param column column to create a ColumnValueExpression from
   * @param alias Table alias used in constructing ColumnValue
   * @param db_oid Database OID
   * @param tbl_oid Table OID for catalog lookup
   * @return column value expression for the underlying column
   */
  static parser::AbstractExpression *GenerateColumnValueExpr(const catalog::Schema::Column &column,
                                                             const parser::AliasType &alias, catalog::db_oid_t db_oid,
                                                             catalog::table_oid_t tbl_oid);

  /**
   * Generate an aggregate expression
   *
   * @param column the underlying column to aggregate over
   * @param aggregate_type the type of aggregation
   * @param distinct whether or not the aggregation should be distinct
   * @param alias Table alias used in constructing ColumnValue
   * @param db_oid Database OID
   * @param tbl_oid Table OID for catalog lookup
   * @return An Aggregate expression with an underlying column
   */
  static parser::AbstractExpression *GenerateAggregateExpr(const catalog::Schema::Column &column,
                                                           parser::ExpressionType aggregate_type, bool distinct,
                                                           const parser::AliasType &alias, catalog::db_oid_t db_oid,
                                                           catalog::table_oid_t tbl_oid);

  /**
   * Generate an aggregate expression with an underlying star expression
   *
   * @param aggregate_type the type of aggregation
   * @param distinct whether or not the aggregation should be distinct
   * @return An Aggregate expression with an underlying star expression
   */
  static parser::AbstractExpression *GenerateStarAggregateExpr(parser::ExpressionType aggregate_type, bool distinct);

  /*
    read wetune rules from wetune table/file
    [info]: it should be used while database start
  */
  void read_wetune_rules(std::string tb_name,std::unordered_map<std::string,Rule*>&wetune_rules);

};

}  // namespace noisepage::optimizer
