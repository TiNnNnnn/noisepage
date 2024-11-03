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
#include "../parser/expression_defs.h"
#include "../parser/expression_util.h"
#include "../parser/expression/column_value_expression.h"

namespace noisepage::optimizer {

    class WeTuneRule : public Rule {
        public:
            WeTuneRule(std::string r,std::unique_ptr<ParsedSqlNode> sql_node){
                constrains_ = sql_node->rule.condtions;
                match_pattern_ =  MakePattern(sql_node->rule.left);
                substitute_ = MakePattern(sql_node->rule.right);
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

                for(auto constrain : constrains_){
                    if(constrain.placeholders.size() != 2){
                        std::cerr<<"bad wetune rule constrain placeholder size: "<<constrain.placeholders.size()<<std::endl;
                        return false;
                    }
                    if(binder_.find(constrain.placeholders[0]) == binder_.end())return false;
                    if(binder_.find(constrain.placeholders[1]) == binder_.end())return false;
                    
                    std::string l = constrain.placeholders[0];
                    std::string r = constrain.placeholders[1];
                    auto l_pattern = binder_.at(l);
                    auto r_pattern = binder_.at(r);

                    if(!InternalCheck(l_pattern,r_pattern,constrain)){
                        return false;
                    }
                }
                return true;
            }
            
            void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                    std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                    OptimizationContext *context) const {
                
            }

            Pattern* MakePattern(WPattern* p){
                if(p == nullptr)return nullptr;
                
                Pattern* new_pattern;
                if(p->type == PatternType::P_INNERJOIN){ 
                    new_pattern = new Pattern(OpType::LOGICALINNERJOIN);
                    auto left = MakePattern(p->children_[0]);
                    auto right = MakePattern(p->children_[1]);
                    new_pattern->AddChild(left);
                    new_pattern->AddChild(right);
                    new_pattern->AddRelOrAttr(p->rel_or_attrs);
                }else if(p->type == PatternType::P_INNERJOIN){
                    new_pattern = new Pattern(OpType::LOGICALLEFTJOIN);
                    auto left = MakePattern(p->children_[0]);
                    auto right = MakePattern(p->children_[1]);
                    new_pattern->AddChild(left);
                    new_pattern->AddChild(right);
                    new_pattern->AddRelOrAttr(p->rel_or_attrs);
                }else if(p->type == PatternType::P_RIGHTJOIN){
                    new_pattern = new Pattern(OpType::LOGICALRIGHTJOIN);
                    auto left = MakePattern(p->children_[0]);
                    auto right = MakePattern(p->children_[1]);
                    new_pattern->AddChild(left);
                    new_pattern->AddChild(right);
                    new_pattern->AddRelOrAttr(p->rel_or_attrs);
                }else if(p->type == PatternType::P_INPUT){
                    new_pattern = new Pattern(OpType::LEAF);
                    new_pattern->AddRelOrAttr(p->rel_or_attrs);
                }else if(p->type == PatternType::P_SEL){
                    new_pattern = new Pattern(OpType::LOGICALFILTER);
                    auto left = MakePattern(p->children_[0]);
                    new_pattern->AddChild(left);
                    new_pattern->AddRelOrAttr(p->rel_or_attrs);
                }else if(p->type == PatternType::P_PROJ){
                    new_pattern = new Pattern(OpType::LOGICALPROJECTION);
                    auto left = MakePattern(p->children_[0]);
                    new_pattern->AddChild(left);
                    new_pattern->AddRelOrAttr(p->rel_or_attrs);
                }else if(p->type == PatternType::P_INSUB){
                    new_pattern = new Pattern(OpType::LOGICALSEMIJOIN);
                    auto left = MakePattern(p->children_[0]);
                    auto right = MakePattern(p->children_[1]);
                    new_pattern->AddChild(left);
                    new_pattern->AddChild(right);
                    new_pattern->AddRelOrAttr(p->rel_or_attrs);
                }else{
                    std::cout<<"error type of pattern"<<std::endl;
                    return nullptr;
                }
                for(size_t i=0;i<p->rel_or_attrs.size();i++){
                    binder_[p->rel_or_attrs[i]] = new_pattern;
                }
                return new_pattern;
            }

            bool InternalCheck(const Pattern* l,const Pattern* r ,ReWriteConstrain constrain) const {
                switch(constrain.type){
                    case RewriteConstrainType::C_AttrsEq:{
                        auto get_attrs = [this,constrain](const Pattern* p) -> std::unordered_set<std::tuple<catalog::col_oid_t,catalog::table_oid_t,catalog::db_oid_t>,TupleHash> {
                            if(p->Type() == OpType::LOGICALINNERJOIN){
                                auto filter_predicates = std::vector<AnnotatedExpression>(p->inner_join_->GetJoinPredicates());
                                return GetJoinAttrs(filter_predicates,p,constrain);
                            }else if(p->Type() == OpType::LOGICALLEFTJOIN){
                                auto filter_predicates = std::vector<AnnotatedExpression>(p->left_join_->GetJoinPredicates());
                                return GetJoinAttrs(filter_predicates,p,constrain);
                            }else if(p->Type() == OpType::LOGICALRIGHTJOIN){
                                auto filter_predicates = std::vector<AnnotatedExpression>(p->right_join_->GetJoinPredicates());
                                return GetJoinAttrs(filter_predicates,p,constrain);
                            }else if(p->Type() == OpType::LOGICALFILTER){
                                auto filter_predicates = std::vector<AnnotatedExpression>(p->filter_->GetPredicates());
                                return GetFilterAttrs(filter_predicates,p,constrain);
                            }else if(p->Type() == OpType::LOGICALSEMIJOIN){
                                auto filter_predicates = std::vector<AnnotatedExpression>(p->semi_join_->GetJoinPredicates());
                                return GetJoinAttrs(filter_predicates,p,constrain);
                            }else if (p->Type() == OpType::LOGICALPROJECTION){
                                return p->proj_;
                            }
                        };
                        //check the list of attr if equal
                        auto l_attr = get_attrs(l);
                        auto r_attr = get_attrs(r);
                        if(l_attr.size() != r_attr.size())return false;
                        for(auto a : l_attr){
                            if(r_attr.find(a) != r_attr.end())return false;
                        }
                        return true;
                    }break;
                    case RewriteConstrainType::C_PredEq:{
                        
                    }break;
                    case RewriteConstrainType::C_SchemaEq:{

                    }break;
                    case RewriteConstrainType::C_RelEq:{

                    }break;
                    case RewriteConstrainType::C_SubAttrs:{

                    }break;
                    case RewriteConstrainType::C_Unique:{

                    }break;
                    case RewriteConstrainType::C_NotNull:{

                    }break;
                    default:{
                        return false;
                    }

                }
                return true;
            }

            bool BindPatternToPlan(common::ManagedPointer<AbstractOptimizerNode>& plan,Pattern* pattern) const {
                if(pattern == nullptr)return true;
                if(pattern->Type() != plan->Contents()->GetOpType()){
                    return false;
                }
                bool ret = true;
                for(size_t i=0;i<pattern->Children().size();i++){
                    auto child = pattern->Children()[i];
                    if(child->Type() == OpType::LOGICALPROJECTION){
                        /**
                         * 1. while meet the join pattern, we need pecial handling.
                         * we directly skip the projection pattern to match its grandson pattern, 
                         * inlcudes filter, join, input. 
                         * 2. proj pattern has no plan contents
                         * 3. through anayle the project pattern's father and son,we can know its attr
                         */
                        ret &= BindPatternToPlan(plan->GetChildren()[i],child->Children()[0]);
                    }else{
                        ret &= BindPatternToPlan(plan->GetChildren()[i],child);
                    }
                }
                match_pattern_->SetContents(plan->Contents());
                switch (match_pattern_->Type()){
                    case OpType::LOGICALINNERJOIN:{
                        match_pattern_->inner_join_ = plan->Contents()->GetContentsAs<LogicalInnerJoin>();
                    }break;
                    case OpType::LOGICALLEFTJOIN:{
                        match_pattern_->left_join_ = plan->Contents()->GetContentsAs<LogicalLeftJoin>();
                    }break;
                    case OpType::LOGICALRIGHTJOIN:{
                        match_pattern_->right_join_ = plan->Contents()->GetContentsAs<LogicalRightJoin>();
                    }break;
                    case OpType::LOGICALFILTER:{
                        match_pattern_->filter_ = plan->Contents()->GetContentsAs<LogicalFilter>();
                    }break;
                    case OpType::LOGICALSEMIJOIN:{
                        match_pattern_->semi_join_ = plan->Contents()->GetContentsAs<LogicalSemiJoin>();
                    }break;
                    case OpType::LOGICALPROJECTION:{
                        //actually no use 
                        //match_pattern_->proj_ = plan->Contents()->GetContentsAs<LogicalProjection>();
                    }break;
                    case OpType::LOGICALGET:{
                        match_pattern_->get_ = plan->Contents()->GetContentsAs<LogicalGet>();
                    }break;
                    default:{
                        std::cerr<<"error type"<<std::endl;
                        return;
                    }
                }
                return ret;
            }

            std::string MakeName(const std::string &input) {
                std::hash<std::string> hash_fn;
                size_t hash_value = hash_fn(input);
                return "LOGICAL_WETUNE_" + std::to_string(hash_value);
            }
        private:
            void BindProjectPattern(Pattern* p) const{
                if(p == nullptr)return;
                for(size_t i=0;i<p->Children().size();i++){
                    auto child = p->Children()[i];
                    if(child->Type() == OpType::LOGICALPROJECTION){
                        auto grandson = child->Children()[0];
                        std::unordered_set<catalog::table_oid_t> tb_oid_set;
                        switch(grandson->Type()){
                            case OpType::LOGICALFILTER:{
                                auto preds = grandson->filter_->GetPredicates();
                                ExprSet col_set;
                                parser::ExpressionUtil::GetTupleValueExprs(&col_set,preds[0].GetExpr());
                                auto e = dynamic_cast<parser::ColumnValueExpression*>((*col_set.begin()).Get());
                                tb_oid_set.insert(e->GetTableOid());
                            }break;
                            case OpType::LOGICALRIGHTJOIN:
                            case OpType::LOGICALLEFTJOIN:
                            case OpType::LOGICALINNERJOIN:{
                                std::vector<AnnotatedExpression> preds; 
                                if(grandson->Type() == OpType::LOGICALINNERJOIN){
                                    preds = grandson->inner_join_->GetJoinPredicates();
                                }else if(grandson->Type() == OpType::LOGICALLEFTJOIN){
                                    preds = grandson->left_join_->GetJoinPredicates();
                                }else{
                                    preds = grandson->right_join_->GetJoinPredicates();
                                }
                                ExprSet col_set;
                                parser::ExpressionUtil::GetTupleValueExprs(&col_set,preds[0].GetExpr());
                                for(auto& col_expr: col_set){
                                    auto e = dynamic_cast<parser::ColumnValueExpression*>(col_expr.Get());
                                    tb_oid_set.insert(e->GetTableOid());    
                                }
                            }break;
                            case OpType::LOGICALGET:{
                                tb_oid_set.insert(p->Children()[0]->get_->GetTableOid());
                            case OpType::LOGICALSEMIJOIN:{
                                auto preds = grandson->semi_join_->GetJoinPredicates();
                                ExprSet col_set;
                                parser::ExpressionUtil::GetTupleValueExprs(&col_set,preds[0].GetExpr());
                                for(auto& col_expr: col_set){
                                    auto e = dynamic_cast<parser::ColumnValueExpression*>(col_expr.Get());
                                    tb_oid_set.insert(e->GetTableOid());    
                                }
                            }
                            }break;
                            default:{
                                std::cerr<<"bad type of join grandson pattern"<<std::endl;
                            }
                        }
                        switch(p->Type()){
                            case OpType::LOGICALRIGHTJOIN:
                            case OpType::LOGICALLEFTJOIN:
                            case OpType::LOGICALINNERJOIN:{
                               std::vector<AnnotatedExpression> preds; 
                                if(grandson->Type() == OpType::LOGICALINNERJOIN){
                                    preds = grandson->inner_join_->GetJoinPredicates();
                                }else if(grandson->Type() == OpType::LOGICALLEFTJOIN){
                                    preds = grandson->left_join_->GetJoinPredicates();
                                }else{
                                    preds = grandson->right_join_->GetJoinPredicates();
                                }
                                ExprSet col_set;
                                parser::ExpressionUtil::GetTupleValueExprs(&col_set,preds[0].GetExpr());
                                for(auto& col_expr: col_set){
                                    auto e = dynamic_cast<parser::ColumnValueExpression*>(col_expr.Get());
                                    if(tb_oid_set.find(e->GetTableOid())!=tb_oid_set.end()){
                                        child->proj_.insert(std::make_tuple(e->GetColumnOid(),e->GetTableOid(),e->GetDatabaseOid()));
                                    } 
                                }
                            }break;
                            case OpType::LOGICALSEMIJOIN:{
                                std::vector<AnnotatedExpression> preds = grandson->semi_join_->GetJoinPredicates();
                                ExprSet col_set;
                                parser::ExpressionUtil::GetTupleValueExprs(&col_set,preds[0].GetExpr());
                                for(auto& col_expr: col_set){
                                    auto e = dynamic_cast<parser::ColumnValueExpression*>(col_expr.Get());
                                    if(tb_oid_set.find(e->GetTableOid())!=tb_oid_set.end()){
                                        child->proj_.insert(std::make_tuple(e->GetColumnOid(),e->GetTableOid(),e->GetDatabaseOid()));
                                    } 
                                }
                            }break;
                            default:{
                                std::cerr<<"bad type of join grandson pattern"<<std::endl;
                            }
                        }
                    }
                    BindProjectPattern(p->Children()[i]);
                }
            }

            std::unordered_set<std::tuple<catalog::col_oid_t,catalog::table_oid_t,catalog::db_oid_t>,TupleHash> GetJoinAttrs(std::vector<noisepage::optimizer::AnnotatedExpression>& preds, const Pattern* p,const ReWriteConstrain& c) const{
                /**
                 * TODO: find the placeholder place in constrainer.
                 */
                std::unordered_set<std::tuple<catalog::col_oid_t,catalog::table_oid_t,catalog::db_oid_t>,TupleHash> attrs;
                /**
                 * if c_pos == 0,then we get attrs of left relation
                 * if c_pos == 1,then we get attrs of right relation
                 */
                int c_pos = -1;
                for(size_t i=0; i < p->GetRelOrAttr().size();i++){
                    for(const auto& e : c.placeholders){
                        if(e == p->GetRelOrAttr()[i]){
                            c_pos = i;
                            break;
                        }
                    }
                }  
                if(c_pos != 0 || c_pos != 1){
                    std::cerr<<"failed to find constraint item in pattern relorattrs"<<std::endl;
                    return attrs;
                } 
                auto GetTableNames = [](const Pattern* p,int idx) -> std::unordered_set<catalog::table_oid_t> {
                    auto child = p->Children()[idx];
                    std::unordered_set<catalog::table_oid_t> tb_oid_set;
                    if(child->Type() == OpType::LOGICALGET){
                        /**
                        * TODO: 11_03,now we just let logicalget as a relation, not a output of subquery,because
                        * if will lead the TableEq(t1,t2) more difficult
                        */
                        tb_oid_set.insert(p->Children()[0]->get_->GetTableOid());
                    }else if (child->Type() == OpType::LOGICALPROJECTION){
                        auto tb_oid = std::get<1>(*child->proj_.begin());
                    }else{
                        std::cerr<<"bad type of join children pattern"<<std::endl;
                    }
                    return tb_oid_set;
                };
                auto tb_sets = GetTableNames(p,c_pos);
                if(tb_sets.size()!=1){
                    std::cerr<<"get tb_sets for join error, tb_sets size = "<<tb_sets.size()<<std::endl;
                }
                for(int i=0;i<preds.size();i++){
                    auto expr = preds[i].GetExpr();
                    switch(expr->GetExpressionType()){
                        case parser::ExpressionType::COMPARE_GREATER_THAN:
                        case parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:
                        case parser::ExpressionType::COMPARE_LESS_THAN:
                        case parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
                        case parser::ExpressionType::COMPARE_NOT_EQUAL:
                        case parser::ExpressionType::COMPARE_EQUAL:{
                            ExprSet col_set;
                            /**
                             * TODO: it may lead to Potential bugs by directly using gettupleexprs,More sound 
                             *       sconsiderations are needed to confirm that this is correct 
                             */
                            parser::ExpressionUtil::GetTupleValueExprs(&col_set,expr);
                            for(auto& col_expr: col_set){
                                auto e = dynamic_cast<parser::ColumnValueExpression*>(col_expr.Get());
                                if(tb_sets.find(e->GetTableOid())!=tb_sets.end()){
                                    attrs.insert(std::make_tuple(e->GetColumnOid(),e->GetTableOid(),e->GetDatabaseOid()));
                                }
                            }
                        }break;    
                    }
                }
            }

            std::unordered_set<std::tuple<catalog::col_oid_t,catalog::table_oid_t,catalog::db_oid_t>,TupleHash> GetFilterAttrs(std::vector<noisepage::optimizer::AnnotatedExpression>& preds, const Pattern* p,const ReWriteConstrain& c) const{
                /** 
                 *TODO: find the placeholder place in constrainer.
                 *for filter , predicate always occuer in the first palce, attr always occuer in the second place ,such as Filter(p0,a4) 
                 *For more ,it seems like Filter pattern always associated with constrain such as AttrsSub(a4,s0)
                */
                std::unordered_set<std::tuple<catalog::col_oid_t,catalog::table_oid_t,catalog::db_oid_t>,TupleHash> attrs;
                for(int i=0;i<preds.size();i++){
                    auto expr = preds[i].GetExpr();
                    switch(expr->GetExpressionType()){
                        case parser::ExpressionType::COMPARE_GREATER_THAN:
                        case parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:
                        case parser::ExpressionType::COMPARE_LESS_THAN:
                        case parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
                        case parser::ExpressionType::COMPARE_NOT_EQUAL:
                        case parser::ExpressionType::COMPARE_EQUAL:{
                            ExprSet col_set;
                            parser::ExpressionUtil::GetTupleValueExprs(&col_set,expr);
                            for(auto& col_expr: col_set){
                                auto e = dynamic_cast<parser::ColumnValueExpression*>(col_expr.Get());
                                attrs.insert(std::make_tuple(e->GetColumnOid(),e->GetTableOid(),e->GetDatabaseOid()));    
                            }
                        }break;    
                    }
                }
                return attrs;
            }
        private:
            /**
             *  A substitute defines the structure of the result after applying the rule
             */
            Pattern* substitute_;
            /**
             *  rule name contains its rulesttr with hash
             */
            std::string name_;
            std::vector<ReWriteConstrain> constrains_;
            std::unordered_map<std::string,Pattern*> binder_;
    };
}