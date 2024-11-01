#pragma once

#include "parse.h"

class ParseStage{
public:
    bool handle_request(std::string rule_str,std::unique_ptr<ParsedSqlNode>& sql_node){
        
        ParsedSqlResult parsed_rule_result;
        parse(rule_str.c_str(),&parsed_rule_result);
        if(parsed_rule_result.sql_nodes().empty()){
            return false;
        }
        if(parsed_rule_result.sql_nodes().size() >1){
            return false;
        }
        sql_node = std::move(parsed_rule_result.sql_nodes().front());
        if(sql_node->flag == SCF_ERROR){
            return false;
        }
        return true;
    }
};