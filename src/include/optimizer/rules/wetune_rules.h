#pragma once

#include <memory>
#include <vector>
#include <unordered_map> 

#include "optimizer/rule.h"

namespace noisepage::optimizer {

    
    
    class WeTuneRule : public Rule {
        WeTuneRule(std::string r){
            //auto ret = ParseRule(r);
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
        bool ParseRule(const std::string& rule_str){
            /*
                here we have two basic ideas to read rule,
                1. create a wetune table with 4 cols (id,pattern,substitute,constraints)
                2. just use simple file to store all the rules
            */
            
        }

        static bool WriteRulesIntoTable(std::string file_name){

        }

        /*
            A substitute defines the structure of the result after applying the rule
        */
        Pattern* substitute_;
        
    };
}