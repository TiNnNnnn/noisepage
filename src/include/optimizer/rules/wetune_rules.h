#pragma once

#include <memory>
#include <vector>
#include <unordered_map> 

#include "optimizer/rule.h"
#include "wetune_parser/parser_defs.h"

namespace noisepage::optimizer {

    class WeTuneRule : public Rule {
        public:
        WeTuneRule(std::string r,std::unique_ptr<ParsedSqlNode> sql_node){
            //auto parse_pattern = sql_node->rulestr.left;    
        }
        
        ~WeTuneRule(){

        }
        
        bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const {
            return true;
        }
        
        void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const {
            

        }
        /*
            A substitute defines the structure of the result after applying the rule
        */
        Pattern* substitute_;
        std::string name_;
    };
}