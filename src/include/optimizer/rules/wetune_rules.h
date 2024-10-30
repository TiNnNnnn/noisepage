#pragma once

#include <memory>
#include <vector>
#include <unordered_map> 

#include "optimizer/rule.h"

namespace noisepage::optimizer {
    
    class WeTuneRule;
    std::unordered_map<std::string,WeTuneRule> wetune_rule_set;
    
    class WeTuneRule : public Rule {
        WeTuneRule(){
            //parser lisp to pattern
            //match_pattern_ = new Pattern(OpType::LOGICALINNERJOIN);
            auto ret = ParseRule()
        }
        
        ~WeTuneRule(){

        }
        
        bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const {
            
        }
        
        void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const {
            

        }
        /*
            parse the similar-lisp style to pattern,substitue
        */
        bool ParseRule(const std::string& rule_str);
        /*
            A substitute defines the structure of the result after applying the rule
        */
        Pattern* substitute_;
        
    };
}