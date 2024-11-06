#pragma once

#include <memory>
#include <vector>
#include <variant>
#include <unordered_map>

#include "optimizer/operator_node_contents.h"
#include "optimizer/logical_operators.h"

namespace noisepage::optimizer {

        // 定义哈希函数对象
struct TupleHash {
  template <typename T>
  std::size_t operator()(const T& tuple) const {
    return std::hash<std::tuple_element_t<0, T>>{}(std::get<0>(tuple)) ^
    std::hash<std::tuple_element_t<1, T>>{}(std::get<1>(tuple)) ^
    std::hash<std::tuple_element_t<2, T>>{}(std::get<2>(tuple));
  }
};

struct PairHash {
    std::size_t operator()(const std::pair<catalog::table_oid_t, catalog::col_oid_t>& key) const {
        std::size_t h1 = std::hash<catalog::table_oid_t>()(key.first);
        std::size_t h2 = std::hash<catalog::col_oid_t>()(key.second);
        return h1 ^ (h2 << 1);
    }
};

/**
 * Class defining a Pattern used for binding
 */
class Pattern {
 public:
  /**
   * Creates a new pattern
   * @param op Operator that node should match
   */
  explicit Pattern(OpType op) : type_(op) {}

  /**
   * Destructor. Deletes all children
   */
  ~Pattern() {
    for (auto child : children_) {
      delete child;
    }
  }

  /**
   * Adds a child to the pattern.
   * Memory control of child passes to this pattern.
   *
   * @param child Pointer to child
   */
  void AddChild(Pattern *child) { children_.push_back(child); }

  void AddRelOrAttr(std::vector<std::string> v){rel_or_attrs_ = v;}
  std::vector<std::string> GetRelOrAttr() const {return rel_or_attrs_;}

  void SetContents(common::ManagedPointer<AbstractOptimizerNodeContents> contents){contents_ = contents;}
  common::ManagedPointer<AbstractOptimizerNodeContents>& GetContents() {return contents_;}

  /**
   * Gets a vector of the children
   * @returns managed children of the pattern node
   */
  const std::vector<Pattern *> &Children() const { return children_; }

  /**
   * Gets number of children
   * @returns number of children
   */
  size_t GetChildPatternsSize() const { return children_.size(); }

  /**
   * Gets the operator this Pattern supposed to represent
   * @returns OpType that Pattern matches against
   */
  OpType Type() const { return type_; }


 public:

    common::ManagedPointer<LogicalInnerJoin> inner_join_;
    common::ManagedPointer<LogicalLeftJoin> left_join_;
    common::ManagedPointer<LogicalRightJoin> right_join_;
    common::ManagedPointer<LogicalFilter> filter_;
    common::ManagedPointer<LogicalSemiJoin> semi_join_;
    common::ManagedPointer<LogicalGet> get_;
    std::unordered_set<std::tuple<catalog::col_oid_t,catalog::table_oid_t,catalog::db_oid_t>,TupleHash> proj_;

    struct LogicalLeaf{
      OpType op;
      common::ManagedPointer<AbstractOptimizerNode> leaf_node_;
    };

    LogicalLeaf leaf_;

 private:
  /**
   * Target Node Type
   */
  OpType type_;

  /**
   * Pattern Children
   */
  std::vector<Pattern *> children_;

  std::vector<std::string>rel_or_attrs_;

  common::ManagedPointer<AbstractOptimizerNodeContents> contents_;

};

}  // namespace noisepage::optimizer
