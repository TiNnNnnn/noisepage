#include "optimizer/rules/wetune_rules.h"

namespace noisepage::optimizer {
            Pattern* WeTuneRule::MakePattern(WPattern* p, std::unordered_set<std::string>& sets){
                if(p == nullptr)return nullptr;
                Pattern* new_pattern;
                if(p->type == PatternType::P_INNERJOIN){ 
                    new_pattern = new Pattern(OpType::LOGICALINNERJOIN);
                    auto left = MakePattern(p->children_[0],sets);
                    auto right = MakePattern(p->children_[1],sets);
                    new_pattern->AddChild(left);
                    new_pattern->AddChild(right);
                    new_pattern->AddRelOrAttr(p->rel_or_attrs);
                }else if(p->type == PatternType::P_INNERJOIN){
                    new_pattern = new Pattern(OpType::LOGICALLEFTJOIN);
                    auto left = MakePattern(p->children_[0],sets);
                    auto right = MakePattern(p->children_[1],sets);
                    new_pattern->AddChild(left);
                    new_pattern->AddChild(right);
                    new_pattern->AddRelOrAttr(p->rel_or_attrs);
                }else if(p->type == PatternType::P_RIGHTJOIN){
                    new_pattern = new Pattern(OpType::LOGICALRIGHTJOIN);
                    auto left = MakePattern(p->children_[0],sets);
                    auto right = MakePattern(p->children_[1],sets);
                    new_pattern->AddChild(left);
                    new_pattern->AddChild(right);
                    new_pattern->AddRelOrAttr(p->rel_or_attrs);
                }else if(p->type == PatternType::P_INPUT){
                    new_pattern = new Pattern(OpType::LEAF);
                    new_pattern->AddRelOrAttr(p->rel_or_attrs);
                }else if(p->type == PatternType::P_SEL){
                    new_pattern = new Pattern(OpType::LOGICALFILTER);
                    auto left = MakePattern(p->children_[0],sets);
                    new_pattern->AddChild(left);
                    new_pattern->AddRelOrAttr(p->rel_or_attrs);
                }else if(p->type == PatternType::P_PROJ){
                    new_pattern = new Pattern(OpType::LOGICALPROJECTION);
                    /**
                     * at most of time, projection means the subquery 
                     */
                    auto left = MakePattern(p->children_[0],sets);
                    new_pattern->AddChild(left);
                    new_pattern->AddRelOrAttr(p->rel_or_attrs);
                }else if(p->type == PatternType::P_INSUB){
                    new_pattern = new Pattern(OpType::LOGICALSEMIJOIN);
                    auto left = MakePattern(p->children_[0],sets);
                    auto right = MakePattern(p->children_[1],sets);
                    new_pattern->AddChild(left);
                    new_pattern->AddChild(right);
                    new_pattern->AddRelOrAttr(p->rel_or_attrs);
                }else{
                    std::cout<<"error type of pattern"<<std::endl;
                    return nullptr;
                }
                for(auto e : p->rel_or_attrs)sets.insert(e);
                for(size_t i=0;i<p->rel_or_attrs.size();i++){
                    binder_[p->rel_or_attrs[i]] = new_pattern;
                }     
                return new_pattern;
            }

            bool WeTuneRule::InternalCheck(const Pattern* l,const Pattern* r ,ReWriteConstrain constrain,const Pattern* e1 , const Pattern* e2) const {
                switch(constrain.type){
                    case RewriteConstrainType::C_RefAttrs:
                    case RewriteConstrainType::C_Unique:
                    case RewriteConstrainType::C_NotNull:
                    case RewriteConstrainType::C_SubAttrs:
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
                                auto exprs = p->proj_->GetExpressions();
                                return GetProjAttrs(exprs,p);
                            }else{
                                return std::unordered_set<std::tuple<catalog::col_oid_t,catalog::table_oid_t,catalog::db_oid_t>,TupleHash>();
                            }
                        };

                        //check the list of attr if equal
                        if(constrain.type == RewriteConstrainType::C_AttrsEq){
                            auto l_attr = get_attrs(l);
                            auto r_attr = get_attrs(r);
                            if(l_attr.size() != r_attr.size())return false;
                            for(auto a : l_attr){
                                if(r_attr.find(a) == r_attr.end())return false;
                            }
                            return true;
                        }else if(constrain.type == RewriteConstrainType::C_SubAttrs){
                            /**
                             * AttrsSub(a,t): if attribute a not found in table's attrs,return false;
                             */
                            auto l_attr = get_attrs(l);
                            std::unordered_set<noisepage::catalog::table_oid_t> tb_oid_set;
                            GetRelFromLeaf(l,tb_oid_set);

                            std::unordered_set<std::pair<catalog::table_oid_t,catalog::col_oid_t>,PairHash> col_oid_set;
                            //get all schemas
                            for(auto rel : tb_oid_set){
                                auto sc = RuleSet::GetCatalogAccessor()->GetSchema(rel);
                                for(auto cs : sc.GetColumns()){
                                    col_oid_set.insert(std::make_pair(rel,cs.Oid()));
                                }
                            }
                            for(auto a : l_attr){
                                if(col_oid_set.find(std::make_pair(std::get<1>(a),std::get<0>(a))) == col_oid_set.end())return false;
                            }
                            return true;
                        }else if(constrain.type == RewriteConstrainType::C_NotNull){
                            /**
                             * NotNull(t0,a0): from catalog
                             */
                            std::unordered_set<noisepage::catalog::table_oid_t> tb_oid_set;
                            GetRelFromLeaf(l,tb_oid_set);
                            auto r_attr = get_attrs(r);

                            std::unordered_map<std::pair<catalog::table_oid_t,catalog::col_oid_t>,bool,PairHash> col_oid_set;
                            //get all schemas
                            for(auto rel : tb_oid_set){
                                auto sc = RuleSet::GetCatalogAccessor()->GetSchema(rel);
                                for(auto cs : sc.GetColumns()){
                                    col_oid_set[std::make_pair(rel,cs.Oid())] = cs.Nullable();
                                }
                            }
                            for(auto a : r_attr){
                                auto iter = col_oid_set.find(std::make_pair(std::get<1>(a),std::get<0>(a)));
                                if(iter == col_oid_set.end())return false;
                                if(iter->second)return false;
                            }
                            return true;
                        }else if(constrain.type == RewriteConstrainType::C_Unique){
                            /** 
                             * Unique(t,a)
                             */
                            std::unordered_set<noisepage::catalog::table_oid_t> tb_oid_set;
                            GetRelFromLeaf(l,tb_oid_set);
                            auto r_attr = get_attrs(r);

                            std::vector<catalog::Schema> scs;
                            std::vector<catalog::IndexSchema> idx_scs;
                            for(auto rel : tb_oid_set){
                                scs.push_back(RuleSet::GetCatalogAccessor()->GetSchema(rel));
                                for(auto idx : RuleSet::GetCatalogAccessor()->GetIndexes(rel)){
                                    idx_scs.push_back(idx.second);
                                }
                            }

                            for(auto attr : r_attr){
                                for(const auto& idx_schema: idx_scs){
                                    //for col with unique , it will has a index 
                                    if(idx_schema.GetIndexedColOids().size() == 1){
                                        bool found = false;
                                        for(const auto& sc : scs){
                                            if(sc.GetColumn(std::get<0>(attr)).Name() == idx_schema.GetColumn(0).Name()){
                                                found = true;
                                                break;
                                            }
                                        }
                                        if(!found)return false;
                                        if(idx_schema.Unique()){
                                            return true;
                                        }
                                    }
                                }
                            }
                            return false;
                        }else if(constrain.type == RewriteConstrainType::C_RefAttrs){
                            auto l_attrs = get_attrs(r);
                            auto r_attrs = get_attrs(e2);
                            std::unordered_set<noisepage::catalog::table_oid_t> left_tb_oid_set;
                            GetRelFromLeaf(l,left_tb_oid_set);
                            std::unordered_set<noisepage::catalog::table_oid_t> right_tb_oid_set;
                            GetRelFromLeaf(e1,left_tb_oid_set);
                            
                           if(left_tb_oid_set.size() != right_tb_oid_set.size())return false;
                           if(l_attrs.size() != r_attrs.size())return false;

                           for(auto oid : left_tb_oid_set){
                                if(right_tb_oid_set.find(oid) == right_tb_oid_set.end())return false;
                           }
                           for(auto attr : l_attrs){
                                if(r_attrs.find(attr) == r_attrs.end())return false;
                           }                            
                           return true;
                        }else{
                            return false;
                        }
                    }break;
                    case RewriteConstrainType::C_PredEq:{
                        /**
                        * we simpliy check if the predicate is totally same,instead of more complex idea
                        */
                        auto get_preds = [this,constrain](const Pattern* p) -> std::vector<noisepage::optimizer::AnnotatedExpression> {
                            if(p->Type() == OpType::LOGICALINNERJOIN){
                                auto filter_predicates = std::vector<AnnotatedExpression>(p->inner_join_->GetJoinPredicates());
                                return filter_predicates;
                            }else if(p->Type() == OpType::LOGICALLEFTJOIN){
                                auto filter_predicates = std::vector<AnnotatedExpression>(p->left_join_->GetJoinPredicates());
                                return filter_predicates;
                            }else if(p->Type() == OpType::LOGICALRIGHTJOIN){
                                auto filter_predicates = std::vector<AnnotatedExpression>(p->right_join_->GetJoinPredicates());
                                return filter_predicates;
                            }else if(p->Type() == OpType::LOGICALFILTER){
                                auto filter_predicates = std::vector<AnnotatedExpression>(p->filter_->GetPredicates());
                                return filter_predicates;
                            }else if(p->Type() == OpType::LOGICALSEMIJOIN){
                                auto filter_predicates = std::vector<AnnotatedExpression>(p->semi_join_->GetJoinPredicates());
                                return filter_predicates;
                            }else{
                                std::cerr<<"error pattern type without preds"<<std::endl;
                                return std::vector<AnnotatedExpression>();
                            }
                        };
                        auto l_preds = get_preds(l);
                        auto r_preds = get_preds(r);
                        if(l_preds.size() != r_preds.size())return false;
                        return CheckPredEqual(l_preds,r_preds);
                    }break;
                    case RewriteConstrainType::C_SchemaEq :{
                        /**
                         * SchemaEq(s_x,s_y): it seems like it only ocurr on Proj<a,s>
                         * 1. it always not used while checking ,but transformer
                         * 2. s may contains more than 1 relations, a may contains more than 1 attrs
                         */
                        auto get_schema = [this](const Pattern* p) -> std::unordered_set<std::tuple<catalog::col_oid_t,catalog::table_oid_t,catalog::db_oid_t>,TupleHash> {
                            if(p->Type() == OpType::LOGICALPROJECTION){
                                auto exprs = p->proj_->GetExpressions();
                                return GetProjAttrs(exprs,p);
                            }
                            return std::unordered_set<std::tuple<catalog::col_oid_t,catalog::table_oid_t,catalog::db_oid_t>,TupleHash>();
                        };
                        std::unordered_set<catalog::Schema,SchemaHash> ls_set;
                        for(const auto& s : get_schema(l)){
                            auto l_sc = RuleSet::GetCatalogAccessor()->GetSchema(std::get<1>(s));
                            ls_set.insert(l_sc);
                        }
                        std::unordered_set<catalog::Schema,SchemaHash> rs_set;
                        for(auto s: get_schema(r)){
                            auto r_sc = RuleSet::GetCatalogAccessor()->GetSchema(std::get<1>(s));
                            ls_set.insert(r_sc);
                        }
                        if(ls_set.size() != rs_set.size()) return false;
                        for(const auto& le : ls_set){
                            if(rs_set.find(le) == rs_set.end())return false;
                        }
                        return true;
                    }break;
                    case RewriteConstrainType::C_RelEq:{
                        /**
                         * TableEq(t_x,t_y): it seems like 't' only ocurr on Input<t>, no matter the shape of the sub plan, it need 
                         * a output based on a single output 
                         */
                        if(l->leaf_.op != r->leaf_.op)return false;
                        return CheckSubPlanEqual(l->leaf_.leaf_node_,r->leaf_.leaf_node_);             
                    }break;
                    default:{
                        return false;
                    }
                }
                return true;
            }

        
            bool WeTuneRule::BindPatternToPlan(common::ManagedPointer<AbstractOptimizerNode>& plan,Pattern* pattern) const {
                if(pattern == nullptr && plan == nullptr)return true;
                if(pattern == nullptr || plan == nullptr)return false;
                if(pattern->Type() != plan->Contents()->GetOpType()){
                    return false;
                }

                bool ret = true;
                for(size_t i=0;i<pattern->Children().size();i++){
                    auto child = pattern->Children()[i];
                    ret &= BindPatternToPlan(plan->GetChildren()[i],child);
                }

                pattern->SetContents(plan->Contents());
                switch (pattern->Type()){
                    case OpType::LOGICALINNERJOIN:{
                        pattern->inner_join_ = plan->Contents()->GetContentsAs<LogicalInnerJoin>();
                    }break;
                    case OpType::LOGICALLEFTJOIN:{
                        pattern->left_join_ = plan->Contents()->GetContentsAs<LogicalLeftJoin>();
                    }break;
                    case OpType::LOGICALRIGHTJOIN:{
                        pattern->right_join_ = plan->Contents()->GetContentsAs<LogicalRightJoin>();
                    }break;
                    case OpType::LOGICALFILTER:{
                        pattern->filter_ = plan->Contents()->GetContentsAs<LogicalFilter>();
                    }break;
                    case OpType::LOGICALSEMIJOIN:{
                        pattern->semi_join_ = plan->Contents()->GetContentsAs<LogicalSemiJoin>();
                    }break;
                    case OpType::LOGICALPROJECTION:{
                       pattern->proj_ = plan->Contents()->GetContentsAs<LogicalProjection>();
                    }break;
                    case OpType::LEAF:{
                        pattern->leaf_.op = plan->Contents()->GetOpType();
                        pattern->leaf_.leaf_node_ = plan;
                    }break;
                    default:{
                        std::cerr<<"error type"<<std::endl;
                        return false;
                    }
                }
                return ret;
            }

            std::unique_ptr<AbstractOptimizerNode> WeTuneRule::BuildRewritePlan(Pattern* p,OptimizationContext *context) const {
                //get associated constrains
                std::vector<ReWriteConstrain> constrains;
                for(const auto& c : trans_constrains_){
                    for(size_t j=0;j<c.placeholders.size();j++){
                        if(binder_.at(c.placeholders[j]) == p){
                            assert(j == 0);
                            constrains.push_back(c);
                        }
                    }
                }
                switch(p->Type()){
                    case OpType::LOGICALINNERJOIN:{
                        std::vector<AnnotatedExpression> join_predicates;
                        for(const  auto &c : constrains){
                            auto l_pattern = binder_.at(c.placeholders[1]);
                            if(c.type == RewriteConstrainType::C_PredEq){
                                join_predicates = l_pattern->filter_->GetPredicates();
                            }else if(c.type == RewriteConstrainType::C_AttrsEq){
                                //do nothing
                            }
                        }

                        std::vector<std::unique_ptr<AbstractOptimizerNode>> c;
                        if(p->Children()[0]->Type() == OpType::LEAF){
                            c.emplace_back(std::move(p->Children()[0]->leaf_.leaf_node_.Get()));
                        }else{
                            auto left_child = BuildRewritePlan(p->Children()[0],context);
                            c.push_back(std::move(left_child));
                        }
                        
                        if(p->Children()[1]->Type() == OpType::LEAF){
                            c.emplace_back(std::move(p->Children()[1]->leaf_.leaf_node_.Get()));
                        }else{
                            auto right_child = BuildRewritePlan(p->Children()[1],context);
                            c.push_back(std::move(right_child));
                        }

                        return  std::make_unique<OperatorNode>(LogicalInnerJoin::Make(std::move(join_predicates))
                                                     .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
                                                 std::move(c), context->GetOptimizerContext()->GetTxn());
                    }break;
                    case OpType::LOGICALLEFTJOIN:{
                        std::vector<AnnotatedExpression> join_predicates;
                        for(const  auto &c : constrains){
                            auto l_pattern = binder_.at(c.placeholders[1]);
                            if(c.type == RewriteConstrainType::C_PredEq){
                                join_predicates = l_pattern->filter_->GetPredicates();
                            }else if(c.type == RewriteConstrainType::C_AttrsEq){
                                //do nothing
                            }
                        }

                        std::vector<std::unique_ptr<AbstractOptimizerNode>> c;
                        if(p->Children()[0]->Type() == OpType::LEAF){
                            c.emplace_back(std::move(p->Children()[0]->leaf_.leaf_node_.Get()));
                        }else{
                            auto left_child = BuildRewritePlan(p->Children()[0],context);
                            c.push_back(std::move(left_child));
                        }
                        
                        if(p->Children()[1]->Type() == OpType::LEAF){
                            c.emplace_back(std::move(p->Children()[1]->leaf_.leaf_node_.Get()));
                        }else{
                            auto right_child = BuildRewritePlan(p->Children()[1],context);
                            c.push_back(std::move(right_child));
                        }

                        return  std::make_unique<OperatorNode>(LogicalLeftJoin::Make(std::move(join_predicates))
                                                     .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
                                                 std::move(c), context->GetOptimizerContext()->GetTxn());
                    }break;
                    case OpType::LOGICALRIGHTJOIN:{
                        std::vector<AnnotatedExpression> join_predicates;
                        for(const  auto &c : constrains){
                            auto l_pattern = binder_.at(c.placeholders[1]);
                            if(c.type == RewriteConstrainType::C_PredEq){
                                join_predicates = l_pattern->filter_->GetPredicates();
                            }else if(c.type == RewriteConstrainType::C_AttrsEq){
                                //do nothing
                            }
                        }

                        std::vector<std::unique_ptr<AbstractOptimizerNode>> c;
                        if(p->Children()[0]->Type() == OpType::LEAF){
                            c.emplace_back(std::move(p->Children()[0]->leaf_.leaf_node_.Get()));
                        }else{
                            auto left_child = BuildRewritePlan(p->Children()[0],context);
                            c.push_back(std::move(left_child));
                        }
                        
                        if(p->Children()[1]->Type() == OpType::LEAF){
                            c.emplace_back(std::move(p->Children()[1]->leaf_.leaf_node_.Get()));
                        }else{
                            auto right_child = BuildRewritePlan(p->Children()[1],context);
                            c.push_back(std::move(right_child));
                        }

                        return  std::make_unique<OperatorNode>(LogicalRightJoin::Make(std::move(join_predicates))
                                                     .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
                                                 std::move(c), context->GetOptimizerContext()->GetTxn());
                    }break;
                    case OpType::LOGICALFILTER:{
                        /**
                         * Filter(p,a), it connect with constrains: PredicateEq(p_x,p_y),AttrEq(a_x,a_y)
                         */
                        std::vector<AnnotatedExpression> predicates;
                        for(const  auto &c : constrains){
                            auto l_pattern = binder_.at(c.placeholders[1]);
                            if(c.type == RewriteConstrainType::C_PredEq){
                                predicates = l_pattern->filter_->GetPredicates();
                            }else if(c.type == RewriteConstrainType::C_AttrsEq){
                                //nothing to do
                            }
                        }

                        std::vector<std::unique_ptr<AbstractOptimizerNode>> c;
                        if(p->Children()[0]->Type() == OpType::LEAF){
                            c.emplace_back(std::move(p->Children()[0]->leaf_.leaf_node_.Get()));
                        }else{
                            auto child = BuildRewritePlan(p->Children()[0],context);
                            c.push_back(std::move(child));
                        }

                        return std::make_unique<OperatorNode>(LogicalFilter::Make(std::move(predicates))
                                                  .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
                                              std::move(c), context->GetOptimizerContext()->GetTxn());
                    }break;
                    case OpType::LOGICALSEMIJOIN:{
                        std::vector<AnnotatedExpression> join_predicates;
                        for(const  auto &c : constrains){
                            auto l_pattern = binder_.at(c.placeholders[1]);
                            if(c.type == RewriteConstrainType::C_PredEq){
                                join_predicates = l_pattern->filter_->GetPredicates();
                            }else if(c.type == RewriteConstrainType::C_AttrsEq){
                                //do nothing
                            }
                        }

                        std::vector<std::unique_ptr<AbstractOptimizerNode>> c;
                        if(p->Children()[0]->Type() == OpType::LEAF){
                            c.emplace_back(std::move(p->Children()[0]->leaf_.leaf_node_.Get()));
                        }else{
                            auto left_child = BuildRewritePlan(p->Children()[0],context);
                            c.push_back(std::move(left_child));
                        }
                        
                        if(p->Children()[1]->Type() == OpType::LEAF){
                            c.emplace_back(std::move(p->Children()[1]->leaf_.leaf_node_.Get()));
                        }else{
                            auto right_child = BuildRewritePlan(p->Children()[1],context);
                            c.push_back(std::move(right_child));
                        }
                        return  std::make_unique<OperatorNode>(LogicalSemiJoin::Make(std::move(join_predicates))
                                                     .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
                                                 std::move(c), context->GetOptimizerContext()->GetTxn());                        
                    }break;
                    case OpType::LOGICALPROJECTION:{
                        std::vector<planner::IndexExpression> expressions;
                        for(const auto &c : constrains){
                            auto l_pattern = binder_.at(c.placeholders[1]);
                            if(c.type == RewriteConstrainType::C_SchemaEq){
                                /**
                                 * FIXME: implement
                                 */
                                expressions = l_pattern->proj_->GetExpressions();
                            }
                        }
                        std::vector<std::unique_ptr<AbstractOptimizerNode>> c;
                        if(p->Children()[0]->Type() == OpType::LEAF){
                            c.emplace_back(std::move(p->Children()[0]->leaf_.leaf_node_.Get()));
                        }else{
                            auto child = BuildRewritePlan(p->Children()[0],context);
                            c.push_back(std::move(child));
                        }
                        return std::make_unique<OperatorNode>(LogicalProjection::Make(std::move(expressions)).RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),std::move(c), context->GetOptimizerContext()->GetTxn());
                    }break;
                    default:{
                        std::cerr<<"error type"<<std::endl;
                        exit(-1);
                    }
                }
                return nullptr;
            }

            void WeTuneRule::GetTransConstrains(){
                //find the attr_or_rel in right but not in left.
                std::unordered_set<std::string>only_right;
                for(auto t : substitute_sets_){
                    if(match_pattern_sets_.find(t) == match_pattern_sets_.end()){
                        only_right.insert(t);
                    }
                }
                std::vector<ReWriteConstrain> new_constrains;
                //classifer constrains into two types;
                for(const auto& c : constrains_){
                    if(only_right.find(c.placeholders[0]) != only_right.end() || only_right.find(c.placeholders[1]) != only_right.end()){
                        trans_constrains_.push_back(c);
                    }else{
                        new_constrains.push_back(c);
                    }
                }
                constrains_ = new_constrains;
            }

            bool WeTuneRule::CheckPredEqual(std::vector<AnnotatedExpression>&l,std::vector<AnnotatedExpression>&r) const {    
                ExprSet l_set;
                ExprSet r_set;
                for(auto e: l)l_set.insert(e.GetExpr());
                for(auto e: r)r_set.insert(e.GetExpr());
                for(auto l_expr: l_set){
                    if(r_set.find(l_expr) == r_set.end())return false;
                }
                return true; 
            }

            void WeTuneRule::GetRelFromProj(const Pattern* plan, std::unordered_set<catalog::table_oid_t>& tb_oid_set) const{
                auto exprs = plan->proj_->GetExpressions();
                auto col_set = GetProjAttrs(exprs,plan);
                for(const auto& col : col_set){
                    tb_oid_set.insert(std::get<1>(col));
                }
            }

            void WeTuneRule::GetRelFromLeaf(const Pattern* sub_plan,std::unordered_set<catalog::table_oid_t>& tb_oid_set) const{
                switch(sub_plan->leaf_.op){
                    case OpType::LOGICALGET:{
                        auto tb_oid = sub_plan->leaf_.leaf_node_->Contents()->GetContentsAs<LogicalGet>()->GetTableOid();
                        tb_oid_set.insert(tb_oid);
                    }break;
                    case OpType::LOGICALFILTER:{
                        auto preds = sub_plan->leaf_.leaf_node_->Contents()->GetContentsAs<LogicalFilter>()->GetPredicates();
                        auto filter_predicates = std::vector<AnnotatedExpression>(preds);
                        auto col_value_expr = dynamic_cast<parser::ColumnValueExpression*>(filter_predicates[0].GetExpr().Get());
                        tb_oid_set.insert(col_value_expr->GetTableOid());
                    }break;
                    case OpType::LOGICALPROJECTION:{
                        auto exprs = sub_plan->leaf_.leaf_node_->Contents()->GetContentsAs<LogicalProjection>()->GetExpressions();
                        auto col_set = GetProjAttrs(exprs,sub_plan);
                        for(const auto& col : col_set){
                            tb_oid_set.insert(std::get<1>(col));
                        }
                    }break;
                    case OpType::LOGICALINNERJOIN:{
                        auto preds = sub_plan->leaf_.leaf_node_->Contents()->GetContentsAs<LogicalInnerJoin>()->GetJoinPredicates();
                        auto join_preds = std::vector<AnnotatedExpression>(preds);
                        ExprSet col_set;
                        parser::ExpressionUtil::GetTupleValueExprs(&col_set,join_preds[0].GetExpr());
                        for(auto& col_expr: col_set){
                            auto e = dynamic_cast<parser::ColumnValueExpression*>(col_expr.Get());
                            tb_oid_set.insert(e->GetTableOid());    
                        }
                    }break;
                    case OpType::LOGICALLEFTJOIN:{
                        auto preds = sub_plan->leaf_.leaf_node_->Contents()->GetContentsAs<LogicalLeftJoin>()->GetJoinPredicates();
                        auto join_preds = std::vector<AnnotatedExpression>(preds);
                        ExprSet col_set;
                        parser::ExpressionUtil::GetTupleValueExprs(&col_set,join_preds[0].GetExpr());
                        for(auto& col_expr: col_set){
                            auto e = dynamic_cast<parser::ColumnValueExpression*>(col_expr.Get());
                            tb_oid_set.insert(e->GetTableOid());    
                        }
                    }break;
                    case OpType::LOGICALRIGHTJOIN:{
                        auto preds = sub_plan->leaf_.leaf_node_->Contents()->GetContentsAs<LogicalRightJoin>()->GetJoinPredicates();
                        auto join_preds = std::vector<AnnotatedExpression>(preds);
                        ExprSet col_set;
                        parser::ExpressionUtil::GetTupleValueExprs(&col_set,join_preds[0].GetExpr());
                        for(auto& col_expr: col_set){
                            auto e = dynamic_cast<parser::ColumnValueExpression*>(col_expr.Get());
                            tb_oid_set.insert(e->GetTableOid());    
                        }
                    }break;
                    case OpType::LOGICALSEMIJOIN:{
                        auto preds = sub_plan->leaf_.leaf_node_->Contents()->GetContentsAs<LogicalSemiJoin>()->GetJoinPredicates();
                        auto join_preds = std::vector<AnnotatedExpression>(preds);
                        ExprSet col_set;
                        parser::ExpressionUtil::GetTupleValueExprs(&col_set,join_preds[0].GetExpr());
                        for(auto& col_expr: col_set){
                            auto e = dynamic_cast<parser::ColumnValueExpression*>(col_expr.Get());
                            tb_oid_set.insert(e->GetTableOid());    
                        }
                    }break;
                    case OpType::LOGICALAGGREGATEANDGROUPBY:{
                        auto exprs = sub_plan->leaf_.leaf_node_->Contents()->GetContentsAs<LogicalAggregateAndGroupBy>()->GetColumns();
                        /**
                         * FIXME:
                         */

                    }
                    default:{
                        std::cerr<<"intercheck error while get rel with unsupport pattern type"<<std::endl;
                        exit(-1);
                    }break;
                }
            }

            std::unordered_set<std::tuple<catalog::col_oid_t,catalog::table_oid_t,catalog::db_oid_t>,TupleHash> WeTuneRule::GetProjAttrs (std::vector<noisepage::planner::IndexExpression>& exprs,const Pattern* p) const{
                std::unordered_set<std::tuple<catalog::col_oid_t,catalog::table_oid_t,catalog::db_oid_t>,TupleHash> attrs;
                for(auto expr : exprs){
                    ExprSet col_set;
                    parser::ExpressionUtil::GetTupleValueExprs(&col_set,expr);
                    for(auto& col_expr: col_set){
                        auto e = dynamic_cast<parser::ColumnValueExpression*>(col_expr.Get());
                        attrs.insert(std::make_tuple(e->GetColumnOid(),e->GetTableOid(),e->GetDatabaseOid()));    
                    }
                }
                return attrs;
            }

            std::unordered_set<std::tuple<catalog::col_oid_t,catalog::table_oid_t,catalog::db_oid_t>,TupleHash> WeTuneRule::GetJoinAttrs(std::vector<noisepage::optimizer::AnnotatedExpression>& preds, const Pattern* p,const ReWriteConstrain& c) const{
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
                auto GetTableNames = [this](const Pattern* p,int idx) -> std::unordered_set<catalog::table_oid_t> {
                    auto child = p->Children()[idx];
                    std::unordered_set<catalog::table_oid_t> tb_oid_set;
                    if(child->Type() == OpType::LEAF){
                        GetRelFromLeaf(child,tb_oid_set);
                    }else if (child->Type() == OpType::LOGICALPROJECTION){
                        GetRelFromProj(p,tb_oid_set);
                    }else{
                        std::cerr<<"bad type of join children pattern"<<std::endl;
                    }
                    return tb_oid_set;
                };
                auto tb_sets = GetTableNames(p,c_pos);
                for(size_t i=0;i<preds.size();i++){
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
                        default:{
                            return attrs;
                        }
                    }
                }
                return attrs;
            }

            std::unordered_set<std::tuple<catalog::col_oid_t,catalog::table_oid_t,catalog::db_oid_t>,TupleHash> WeTuneRule::GetFilterAttrs(std::vector<noisepage::optimizer::AnnotatedExpression>& preds, const Pattern* p,const ReWriteConstrain& c) const{
                /** 
                 *TODO: find the placeholder place in constrainer.
                 *for filter , predicate always occuer in the first palce, attr always occuer in the second place ,such as Filter(p0,a4) 
                 *For more ,it seems like Filter pattern always associated with constrain such as AttrsSub(a4,s0)
                */
                std::unordered_set<std::tuple<catalog::col_oid_t,catalog::table_oid_t,catalog::db_oid_t>,TupleHash> attrs;
                for(size_t i=0;i<preds.size();i++){
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
                        default:{
                            return attrs;
                        } 
                    }
                }
                return attrs;
            }

            bool WeTuneRule::CheckSubPlanEqual(const common::ManagedPointer<AbstractOptimizerNode>& left,const common::ManagedPointer<AbstractOptimizerNode>& right) const{
                if(left == nullptr && right == nullptr)return true;
                if(left == nullptr || right == nullptr)return false;
                if(left->Contents()->GetOpType() != right->Contents()->GetOpType())return false;

                bool res = false;
                switch(left->Contents()->GetOpType()){
                    case OpType::LOGICALINNERJOIN: {
                        auto l = left->Contents()->GetContentsAs<LogicalInnerJoin>();
                        auto r = left->Contents()->GetContentsAs<LogicalInnerJoin>();
                        if(*l != *r)return false;
                        res = CheckSubPlanEqual(left->GetChildren()[0],right->GetChildren()[0]) && CheckSubPlanEqual(left->GetChildren()[1],right->GetChildren()[1]);
                    }break;
                    case OpType::LOGICALLEFTJOIN: {
                        auto l = left->Contents()->GetContentsAs<LogicalLeftJoin>();
                        auto r = left->Contents()->GetContentsAs<LogicalLeftJoin>();
                        if(*l != *r)return false; 
                        res = CheckSubPlanEqual(left->GetChildren()[0],right->GetChildren()[0]) && CheckSubPlanEqual(left->GetChildren()[1],right->GetChildren()[1]);
                    }break;
                    case OpType::LOGICALRIGHTJOIN: {
                        auto l = left->Contents()->GetContentsAs<LogicalRightJoin>();
                        auto r = left->Contents()->GetContentsAs<LogicalRightJoin>();
                        if(*l != *r)return false;
                        res = CheckSubPlanEqual(left->GetChildren()[0],right->GetChildren()[0]) && CheckSubPlanEqual(left->GetChildren()[1],right->GetChildren()[1]);
                    }break;              
                    case OpType::LOGICALSEMIJOIN: {
                        auto l = left->Contents()->GetContentsAs<LogicalSemiJoin>();
                        auto r = left->Contents()->GetContentsAs<LogicalSemiJoin>();
                        if(*l != *r)return false;
                        res = CheckSubPlanEqual(left->GetChildren()[0],right->GetChildren()[0]) && CheckSubPlanEqual(left->GetChildren()[1],right->GetChildren()[1]);
                    }break;
                    case OpType::LOGICALMARKJOIN: {
                        auto l = left->Contents()->GetContentsAs<LogicalMarkJoin>();
                        auto r = left->Contents()->GetContentsAs<LogicalMarkJoin>();
                        if(*l != *r)return false;
                        res = CheckSubPlanEqual(left->GetChildren()[0],right->GetChildren()[0]) && CheckSubPlanEqual(left->GetChildren()[1],right->GetChildren()[1]);                        
                    }break;
                    case OpType::LOGICALOUTERJOIN: {
                        auto l = left->Contents()->GetContentsAs<LogicalOuterJoin>();
                        auto r = left->Contents()->GetContentsAs<LogicalOuterJoin>();
                        if(*l != *r)return false;
                        res = CheckSubPlanEqual(left->GetChildren()[0],right->GetChildren()[0]) && CheckSubPlanEqual(left->GetChildren()[1],right->GetChildren()[1]);                        
                    }break;
                    case OpType::LOGICALSINGLEJOIN: {
                        auto l = left->Contents()->GetContentsAs<LogicalSingleJoin>();
                        auto r = left->Contents()->GetContentsAs<LogicalSingleJoin>();
                        if(*l != *r)return false;
                        res = CheckSubPlanEqual(left->GetChildren()[0],right->GetChildren()[0]) && CheckSubPlanEqual(left->GetChildren()[1],right->GetChildren()[1]);                        
                    }break;
                    case OpType::LOGICALDEPENDENTJOIN: {
                        auto l = left->Contents()->GetContentsAs<LogicalDependentJoin>();
                        auto r = left->Contents()->GetContentsAs<LogicalDependentJoin>();
                        if(*l != *r)return false;
                        res = CheckSubPlanEqual(left->GetChildren()[0],right->GetChildren()[0]) && CheckSubPlanEqual(left->GetChildren()[1],right->GetChildren()[1]);                        
                    }break;
                    case OpType::LOGICALAGGREGATEANDGROUPBY:{
                        auto l = left->Contents()->GetContentsAs<LogicalAggregateAndGroupBy>();
                        auto r = left->Contents()->GetContentsAs<LogicalAggregateAndGroupBy>();
                        if(*l != *r)return false;
                        res = CheckSubPlanEqual(left->GetChildren()[0],right->GetChildren()[0]);                                                
                    }break;
                    case OpType::LOGICALFILTER:{
                        auto l = left->Contents()->GetContentsAs<LogicalFilter>();
                        auto r = left->Contents()->GetContentsAs<LogicalFilter>();
                        if(*l != *r)return false;
                        res = CheckSubPlanEqual(left->GetChildren()[0],right->GetChildren()[0]);                              
                    }break;
                    case OpType::LOGICALEXTERNALFILEGET:{
                        auto l = left->Contents()->GetContentsAs<LogicalExternalFileGet>();
                        auto r = left->Contents()->GetContentsAs<LogicalExternalFileGet>();
                        if(*l != *r)return false;
                        res = CheckSubPlanEqual(left->GetChildren()[0],right->GetChildren()[0]);                          
                    }break;
                    case OpType::LOGICALGET:{
                        auto l = left->Contents()->GetContentsAs<LogicalGet>();
                        auto r = left->Contents()->GetContentsAs<LogicalGet>();
                        if(*l != *r)return false;
                        res = true;                  
                    }break;
                    case OpType::LOGICALUNION:{
                        auto l = left->Contents()->GetContentsAs<LogicalUnion>();
                        auto r = left->Contents()->GetContentsAs<LogicalUnion>();
                        if(*l != *r)return false;
                        res = CheckSubPlanEqual(left->GetChildren()[0],right->GetChildren()[0]);                                   
                    }break;
                    case OpType::LOGICALLIMIT:{
                        auto l = left->Contents()->GetContentsAs<LogicalLimit>();
                        auto r = left->Contents()->GetContentsAs<LogicalLimit>();
                        if(*l != *r)return false;
                        res = CheckSubPlanEqual(left->GetChildren()[0],right->GetChildren()[0]);                                   
                    }break;
                    case OpType::LOGICALPROJECTION:{
                        auto l = left->Contents()->GetContentsAs<LogicalProjection>();
                        auto r = left->Contents()->GetContentsAs<LogicalProjection>();
                        if(*l != *r)return false;
                        res = CheckSubPlanEqual(left->GetChildren()[0],right->GetChildren()[0]);           
                    }break;
                    case OpType::LOGICALQUERYDERIVEDGET:{
                        auto l = left->Contents()->GetContentsAs<LogicalQueryDerivedGet>();
                        auto r = left->Contents()->GetContentsAs<LogicalQueryDerivedGet>();
                        if(*l != *r)return false;
                        res = CheckSubPlanEqual(left->GetChildren()[0],right->GetChildren()[0]);                         
                    }break;
                    default:{
                        std::cerr<<"error typr while check sub plan equality"<<std::endl;
                        exit(-1);
                    }
                }
                return res;
            }
}