/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Meiyi
//

#pragma once

#include <string>
#include <vector>
#include <memory>

//#include "common/value.h"

class Expression;

/**
 * @defgroup SQLParser SQL Parser
 */

enum RewriteConstrainType{
    C_RelEq,
    C_AttrsEq,
    C_PredEq,
    C_SubAttrs,
    C_SchemaEq,
    C_Unique,
    C_NotNull,
};

struct ReWriteConstrain{
    RewriteConstrainType type;
    std::vector<std::string> placeholders;
    bool for_transformer;
};


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

struct Pattern {
  //virtual std::vector<std::string> GetInfo();
  PatternType type;
  std::vector<std::string> rel_or_attrs;
  //just for Proj
  bool distinct;
  std::vector<Pattern *> children_;
};

struct RuleNode {
  Pattern* left;
  Pattern* right;
  std::vector<ReWriteConstrain> condtions;
};

/**
 * @brief 解析SQL语句出现了错误
 * @ingroup SQLParser
 * @details 当前解析时并没有处理错误的行号和列号
 */
struct ErrorSqlNode
{
  std::string error_msg;
  int         line;
  int         column;
};

/**
 * @brief 表示一个SQL语句的类型
 * @ingroup SQLParser
 */
enum SqlCommandFlag
{
  SCF_ERROR = 0,
  SCF_RULE,
};

/**
 * @brief 表示一个SQL语句
 * @ingroup SQLParser
 */
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

/**
 * @brief 表示语法解析后的数据
 * @ingroup SQLParser
 */
class ParsedSqlResult
{
public:
  void add_sql_node(std::unique_ptr<ParsedSqlNode> sql_node);

  std::vector<std::unique_ptr<ParsedSqlNode>> &sql_nodes() { return sql_nodes_; }

private:
  std::vector<std::unique_ptr<ParsedSqlNode>> sql_nodes_;  ///< 这里记录SQL命令。虽然看起来支持多个，但是当前仅处理一个
};