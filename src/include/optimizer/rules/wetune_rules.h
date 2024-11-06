#pragma once

#include <memory>
#include <vector>
#include <unordered_map> 
#include <ostream>
#include <variant>
#include <memory>
#include <functional>
#include "optimizer/rule.h"
#include "wetune_parser/parser_defs.h"
#include "parser/expression_defs.h"
#include "parser/expression_util.h"
#include "parser/expression/column_value_expression.h"
#include "catalog/catalog_accessor.h"
#include "optimizer/optimizer_context.h"

namespace noisepage::optimizer {
    /**
     * TODO: before apply wetune rules, we need first apply unnnesting rules
     */
    class WeTuneRule : public Rule {
        public:
            WeTuneRule(std::string r,std::unique_ptr<ParsedSqlNode> sql_node){
                constrains_ = sql_node->rule.condtions;
                match_pattern_ =  MakePattern(sql_node->rule.left,match_pattern_sets_);
                substitute_ = MakePattern(sql_node->rule.right,substitute_sets_);
                GetTransConstrains();
                name_ = MakeName(r);
            }
            
            ~WeTuneRule(){
                delete(match_pattern_);
                delete(substitute_);
            }
            
            bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const {
                //bind pattern with logical plan
                if(!BindPatternToPlan(plan,match_pattern_))return false;
                //padding projection pattern attr info
                BindProjectPattern(match_pattern_);
                //check the constrains
                for(auto constrain : constrains_){
                    if(constrain.placeholders.size() != 2 || constrain.placeholders.size() != 4){
                        std::cerr<<"bad wetune rule constrain placeholder size: "<<constrain.placeholders.size()<<std::endl;
                        return false;
                    }
                    if(binder_.find(constrain.placeholders[0]) == binder_.end())return false;
                    if(binder_.find(constrain.placeholders[1]) == binder_.end())return false;
                    if(constrain.type != RewriteConstrainType::C_RefAttrs){
                        std::string l = constrain.placeholders[0];
                        std::string r = constrain.placeholders[1];
                        auto l_pattern = binder_.at(l);
                        auto r_pattern = binder_.at(r);

                        if(!InternalCheck(l_pattern,r_pattern,constrain)){
                            return false;
                        }
                    }else{
                        if(binder_.find(constrain.placeholders[2]) == binder_.end())return false;
                        if(binder_.find(constrain.placeholders[3]) == binder_.end())return false;

                    }

                }
                return true;
            }
            
            void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                    std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                    OptimizationContext *context) const {
                auto root = BuildRewritePlan(substitute_,context);   
                transformed->push_back(root); 
            }
            
        private:
            Pattern* MakePattern(WPattern* p, std::unordered_set<std::string>& sets);
            
            bool InternalCheck(const Pattern* l,const Pattern* r ,ReWriteConstrain constrain,const Pattern* e1 = nullptr, const Pattern* e2 = nullptr) const;

            bool BindPatternToPlan(common::ManagedPointer<AbstractOptimizerNode>& plan,Pattern* pattern) const;
            std::unique_ptr<AbstractOptimizerNode> BuildRewritePlan(Pattern* p,OptimizationContext *context) const;
            void GetTransConstrains();
            bool CheckPredEqual(std::vector<AnnotatedExpression>&l,std::vector<AnnotatedExpression>&r) const;
            void BindProjectPattern(Pattern* p) const;
            void GetRelFromLeaf(const Pattern* sub_plan,std::unordered_set<catalog::table_oid_t>& tb_oid_set) const;
            bool CheckSubPlanEqual(const common::ManagedPointer<AbstractOptimizerNode>& left,const common::ManagedPointer<AbstractOptimizerNode>& right) const;
            
            std::unordered_set<std::tuple<catalog::col_oid_t,catalog::table_oid_t,catalog::db_oid_t>,TupleHash> GetJoinAttrs(std::vector<noisepage::optimizer::AnnotatedExpression>& preds, const Pattern* p,const ReWriteConstrain& c) const;
            std::unordered_set<std::tuple<catalog::col_oid_t,catalog::table_oid_t,catalog::db_oid_t>,TupleHash> GetFilterAttrs(std::vector<noisepage::optimizer::AnnotatedExpression>& preds, const Pattern* p,const ReWriteConstrain& c) const;
            
            std::string MakeName(const std::string &input) {
                std::hash<std::string> hash_fn;
                size_t hash_value = hash_fn(input);
                return "LOGICAL_WETUNE_" + std::to_string(hash_value);
            }
        private:
            // A substitute defines the structure of the result after applying the rule
            Pattern* substitute_;
            //rule name contains its rulesttr with hash
            std::string name_;
            //divide constrains into two types
            std::vector<ReWriteConstrain> constrains_;
            std::vector<ReWriteConstrain> trans_constrains_;
            //binder
            std::unordered_map<std::string,Pattern*> binder_;
            //collect the constrains placeholders
            std::unordered_set<std::string> match_pattern_sets_;
            std::unordered_set<std::string> substitute_sets_;
    };
}