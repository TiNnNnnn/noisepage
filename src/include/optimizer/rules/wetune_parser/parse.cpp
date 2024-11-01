#include "parse.h"


bool parse(char *st, ParsedSqlNode*sqln);

ParsedSqlNode::ParsedSqlNode() : flag(SCF_ERROR){}

ParsedSqlNode::ParsedSqlNode(SqlCommandFlag _flag):flag(_flag){}

void ParsedSqlResult::add_sql_node(std::unique_ptr<ParsedSqlNode> sql_node)
{
    sql_nodes_.push_back(std::move(sql_node));
}

int sql_parse(const char *st,ParsedSqlResult*sql_result);

bool parse(const char* s,ParsedSqlResult *sql_result){
    sql_parse(s,sql_result);
    return true;
}