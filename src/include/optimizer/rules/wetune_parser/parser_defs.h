#pragma once

#include <string>
#include <vector>
#include <memory>

class Expression;


/**constrain types of rewriting rules*/
enum RewriteConstrainType{
    C_RelEq,
    C_AttrsEq,
    C_PredEq,
    C_SubAttrs,
    C_SchemaEq,
    C_Unique,
    C_NotNull,
    C_RefAttrs,
};

struct ReWriteConstrain{
    RewriteConstrainType type;
    std::vector<std::string> placeholders;
    bool for_transformer;
};

/**type of pattern in wetune*/
enum PatternType{
  P_INPUT,
  P_PROJ,
  P_SEL,
  P_INSUB,
  P_LEFTJOIN,
  P_RIGHTJOIN,
  P_INNERJOIN,
  P_DEDUP,
};

struct WPattern {
public:
  //virtual std::vector<std::string> GetInfo();
  PatternType type;
  std::vector<std::string> rel_or_attrs;
  /*for project */
  bool distinct;
  std::vector<WPattern *> children_;
};

/**
 * RuleNode,descirbe a wetune format rule
 */
struct RuleNode {
  WPattern* left;
  WPattern* right;
  std::vector<ReWriteConstrain> condtions;
};

struct ErrorSqlNode
{
  std::string error_msg;
  int         line;
  int         column;
};

enum SqlCommandFlag
{
  SCF_ERROR = 0,
  SCF_RULE,
};

class ParsedSqlNode
{
public:
  enum SqlCommandFlag flag;
  ErrorSqlNode        error;
  RuleNode         rule;
public:
  ParsedSqlNode();
  explicit ParsedSqlNode(SqlCommandFlag flag);
};

class ParsedSqlResult
{
public:
  void add_sql_node(std::unique_ptr<ParsedSqlNode> sql_node);

  std::vector<std::unique_ptr<ParsedSqlNode>> &sql_nodes() { return sql_nodes_; }

private:
  std::vector<std::unique_ptr<ParsedSqlNode>> sql_nodes_; 
};