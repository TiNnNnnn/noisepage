%{

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <algorithm>

#include "parser_defs.h"
#include "yacc_rule.hpp"
#include "lex_rule.h"

using namespace std;

string token_name(const char *sql_string, YYLTYPE *llocp)
{
  return string(sql_string + llocp->first_column, llocp->last_column - llocp->first_column + 1);
}

int yyerror(YYLTYPE *llocp, const char *sql_string, ParsedSqlResult *sql_result, yyscan_t scanner, const char *msg)
{
  std::unique_ptr<ParsedSqlNode> error_sql_node = std::make_unique<ParsedSqlNode>(SCF_ERROR);
  error_sql_node->error.error_msg = msg;
  error_sql_node->error.line = llocp->first_line;
  error_sql_node->error.column = llocp->first_column;
  sql_result->add_sql_node(std::move(error_sql_node));
  return 0;
}

%}

%define api.pure full
%define parse.error verbose
/** 启用位置标识 **/
%locations
%lex-param { yyscan_t scanner }
/** 这些定义了在yyparse函数中的参数 **/
%parse-param { const char * sql_string }
%parse-param { ParsedSqlResult * sql_result }
%parse-param { void * scanner }

//标识tokens
%token  SEMICOLON
        BY
        CREATE
        DROP
        GROUP
        TABLE
        TABLES
        INDEX
        CALC
        SELECT
        DESC
        SHOW
        SYNC
        INSERT
        DELETE
        UPDATE
        LBRACE
        RBRACE
        LBRACKET
        RBRACKET
        COMMA
        TRX_BEGIN
        TRX_COMMIT
        TRX_ROLLBACK
        INT_T
        STRING_T
        FLOAT_T
        DATE_T
        VECTOR_T
        HELP
        EXIT
        DOT //QUOTE
        INTO
        VALUES
        FROM
        WHERE
        AND
        SET
        ON
        LOAD
        INFILE
        EXPLAIN
        STORAGE
        FORMAT
        DATA 
        EQ
        LT
        GT
        LE
        GE
        NE
        LIKE
        L2_DISTANCE
        COSINE_DISTANCE
        INNER_DISTANCE
        INNER
        JOIN 
        IN
        EXSIST
        NOT
        UNIQUE
        NULLABLE
        PRIMARY
        KEY
        BAR

        TABLEEQ
        ATTRSEQ                                  
        PREDICATEEQ
        SCHEMAEQ                             
        ATTRSSUB                                
        REFERENCE                                
        NOTNULL

        LEFTJOIN
        RIGHTJOIN
        INNERJOIN
        INPUT
        PROJ
        INSUBFILTER
        FILTER 

        
/** union 中定义各种数据类型，真实生成的代码也是union类型，所以不能有非POD类型的数据 **/
%union {
  ParsedSqlNode *sql_node;
  WPattern* pattern;
  ReWriteConstrain * cs;
  std::vector<ReWriteConstrain>* cs_list;
  char* string;
  bool        boolean;
  int  number;
  float   floats;
}

%token <number> NUMBER
%token <floats> FLOAT
%token <string> ID
%token <string> SSS

//非终结符
/** type 定义了各种解析后的结果输出的是什么类型。类型对应了 union 中的定义的成员变量名称 **/
%type<sql_node>  rule_stmt
%type<sql_node>  command_wrapper
%type<sql_node>  commands
%type<pattern>   pattern 
%type<cs>        constrain
%type<cs_list>   constrain_list
%type<boolean>   opt_star
%%

commands: command_wrapper opt_semicolon  //commands or sqls. parser starts here.
  {
    std::unique_ptr<ParsedSqlNode> sql_node = std::unique_ptr<ParsedSqlNode>($1);
    sql_result->add_sql_node(std::move(sql_node));
  }
  ;

command_wrapper:
  rule_stmt
  ;

constrain:
  TABLEEQ LBRACE ID COMMA ID RBRACE
  {
    $$ = new ReWriteConstrain();
    $$->type = RewriteConstrainType::C_RelEq;
    $$->placeholders.push_back($3);
    $$->placeholders.push_back($5);
  }
  | ATTRSEQ LBRACE ID COMMA ID RBRACE
  {
    $$ = new ReWriteConstrain();
    $$->type = RewriteConstrainType::C_AttrsEq;
    $$->placeholders.push_back($3);
    $$->placeholders.push_back($5);
  }
  | ATTRSSUB LBRACE ID COMMA ID RBRACE
  {
    $$ = new ReWriteConstrain();
    $$->type = RewriteConstrainType::C_SubAttrs;
    $$->placeholders.push_back($3);
    $$->placeholders.push_back($3);
  }
  | PREDICATEEQ LBRACE ID COMMA ID RBRACE
  {
    $$ = new ReWriteConstrain();
    $$->type = RewriteConstrainType::C_PredEq;
    $$->placeholders.push_back($3);
    $$->placeholders.push_back($5);
  }
  | SCHEMAEQ LBRACE ID COMMA ID RBRACE
  {
    $$ = new ReWriteConstrain();
    $$->type = RewriteConstrainType::C_SchemaEq;
    $$->placeholders.push_back($3);
    $$->placeholders.push_back($5);
  }
  | NOTNULL LBRACE ID COMMA ID RBRACE
  {
    $$ = new ReWriteConstrain();
    $$->type = RewriteConstrainType::C_NotNull;
    $$->placeholders.push_back($3);
    $$->placeholders.push_back($5);    
  }
  | UNIQUE LBRACE ID COMMA ID RBRACE
  {
    $$ = new ReWriteConstrain();
    $$->type = RewriteConstrainType::C_Unique;
    $$->placeholders.push_back($3);
    $$->placeholders.push_back($5);
  }
  | REFERENCE LBRACE ID COMMA ID COMMA ID COMMA ID RBRACE
  {
    $$ = new ReWriteConstrain();
    $$->type = RewriteConstrainType::C_RefAttrs;
    $$->placeholders.push_back($3);
    $$->placeholders.push_back($5);
    $$->placeholders.push_back($7);
    $$->placeholders.push_back($9);
  }
  ;

constrain_list:
  constrain
  {
    $$ = new std::vector<ReWriteConstrain>();
    $$->push_back(*$1);
  }
  |constrain_list ';' constrain {
    $$->push_back (*$3);
  }
  ;


pattern:
  LEFTJOIN '<' ID ID '>' LBRACE pattern COMMA pattern RBRACE
  {
    $$ = new WPattern();
    $$->type = PatternType::P_LEFTJOIN;
    $$->rel_or_attrs.push_back($3);
    $$->rel_or_attrs.push_back($4);
    $$->children_.push_back($7);
    $$->children_.push_back($9);
  }
  | RIGHTJOIN '<' ID ID '>' LBRACE pattern COMMA pattern RBRACE
  {
    $$ = new WPattern();
    $$->type = PatternType::P_RIGHTJOIN;
    $$->rel_or_attrs.push_back($3);
    $$->rel_or_attrs.push_back($4);
    $$->children_.push_back($7);
    $$->children_.push_back($9);
  }
  | INNERJOIN '<' ID ID '>' LBRACE pattern COMMA pattern RBRACE
  {
    $$ = new WPattern();
    $$->type = PatternType::P_INNERJOIN;
    $$->rel_or_attrs.push_back($3);
    $$->rel_or_attrs.push_back($4);
    $$->children_.push_back($7);
    $$->children_.push_back($9);
  }
  | INPUT '<' ID '>'
  {
    $$ = new WPattern();
    $$->type = PatternType::P_INPUT;
    $$->rel_or_attrs.push_back($3);
  }
  | PROJ opt_star '<' ID ID '>' LBRACE pattern RBRACE
  {
    $$ = new WPattern();
    $$->type = PatternType::P_PROJ;
    $$->distinct = $2;
    $$->rel_or_attrs.push_back($4);
    $$->rel_or_attrs.push_back($5);
    $$->children_.push_back($8);
  }
  | INSUBFILTER '<' ID '>' LBRACE pattern COMMA pattern RBRACE
  {
    $$ = new WPattern();
    $$->type = PatternType::P_INSUB;
    $$->rel_or_attrs.push_back($3);
    $$->children_.push_back($6);
    $$->children_.push_back($8);
  }
  | FILTER '<' ID COMMA ID '>' LBRACE pattern RBRACE
  {
    $$ = new WPattern();
    $$->type = PatternType::P_SEL;
    $$->rel_or_attrs.push_back($3);
    $$->rel_or_attrs.push_back($5);
    $$->children_.push_back($8);
  }
  ;
opt_star:
  {
    $$ = false;
  }
  | '*'
  {
    $$ = true;
  }
  ;


rule_stmt:
  pattern BAR pattern BAR constrain_list
  {
    $$ = new ParsedSqlNode(SCF_RULE);
    $$->rule.left = $1;
    $$->rule.right = $3;
    $$->rule.condtions = *$5;
  }
  ;

opt_semicolon: /*empty*/
    | SEMICOLON
    ;

%%
//_____________________________________________________________________
extern void scan_string(const char *str, yyscan_t scanner);

int sql_parse(const char *s, ParsedSqlResult *sql_result) {
  yyscan_t scanner;
  yylex_init(&scanner);
  scan_string(s, scanner);
  int result = yyparse(s, sql_result, scanner);
  yylex_destroy(scanner);
  return result;
}