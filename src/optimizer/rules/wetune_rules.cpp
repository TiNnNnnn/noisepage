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

            bool WeTuneRule::InternalCheck(const Pattern* l,const Pattern* r ,ReWriteConstrain constrain) const {
                switch(constrain.type){
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
                                return p->proj_;
                            }else{
                                return std::unordered_set<std::tuple<catalog::col_oid_t,catalog::table_oid_t,catalog::db_oid_t>,TupleHash>();
                            }
                        };

                        auto get_rel = [this](const Pattern* p) -> std::pair<catalog::db_oid_t,catalog::table_oid_t> {
                            // rel is all from the  pattern: input<t>, no matter the shape of the sub plan, it need 
                            // a output based on a single output 
                            if(p->Type() == OpType::LEAF){
                                auto type = p->leaf_.op;
                            }else{
                                std::cerr<<"intercheck error while get rel with impossible pattern type"<<std::endl;
                                exit(-1);
                            }
                            // if(p->Type() == OpType::LOGICALGET){
                            //     return std::make_pair(p->get_->GetDatabaseOid(),p->get_->GetTableOid());
                            // }
                            return std::make_pair(catalog::INVALID_DATABASE_OID,catalog::INVALID_TABLE_OID);
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
                            auto r_rel = get_rel(r);
                            std::unordered_set<catalog::col_oid_t> col_oid_set;
                            auto ts = RuleSet::GetCatalogAccessor()->GetTableStatistics(r_rel.second);
                            for(auto cs : ts.GetColumnStats()){
                                col_oid_set.insert(cs->GetColumnID());
                            }
                            for(auto a : l_attr){
                                if(col_oid_set.find(std::get<0>(a)) == col_oid_set.end())return false;
                            }
                            return true;
                        }else if(constrain.type == RewriteConstrainType::C_NotNull){
                            /**
                             * NotNull(t0,a0): from catalog
                             */
                            auto l_rel =get_rel(l);
                            auto r_attr = get_attrs(r);

                            std::unordered_map<catalog::col_oid_t,bool> col_oid_set;
                            auto sc = RuleSet::GetCatalogAccessor()->GetSchema(l_rel.second);
                            for(auto cs : sc.GetColumns()){
                                col_oid_set[cs.Oid()] = cs.Nullable();
                            }
                            for(auto a : r_attr){
                                auto iter = col_oid_set.find(std::get<0>(a));
                                if(iter == col_oid_set.end())return false;
                                if(iter->second)return false;
                            }
                            return true;
                        }else if(constrain.type == RewriteConstrainType::C_Unique){
                            /** 
                             * Unique(t,a)
                             */
                            auto l_rel =get_rel(l);
                            auto r_attr = get_attrs(r);
                            auto sc = RuleSet::GetCatalogAccessor()->GetSchema(l_rel.second);
                            auto idxs = RuleSet::GetCatalogAccessor()->GetIndexes(l_rel.second);
                            for(auto attr : r_attr){
                                for(auto idx: idxs){
                                    auto idx_schema = idx.second;
                                    if(idx_schema.GetColumns().size() == 1 && idx_schema.GetColumn(0).Name() == sc.GetColumn(std::get<0>(attr)).Name()){
                                        if(idx_schema.Unique()){
                                            return true;
                                        }
                                    }
                                }
                            }
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
                         * SchemaEq(s_x,s_y): it seems like it only ocurr on Proj<a,s> ,and not used while checking ,but transformer
                         */
                        auto get_schema = [this](const Pattern* p) -> std::unordered_set<std::tuple<catalog::col_oid_t,catalog::table_oid_t,catalog::db_oid_t>,TupleHash> {
                            if(p->Type() == OpType::LOGICALPROJECTION){
                                return p->proj_;
                            }
                            return std::unordered_set<std::tuple<catalog::col_oid_t,catalog::table_oid_t,catalog::db_oid_t>,TupleHash>();
                        };
                        auto l_schema = get_schema(l);
                        auto r_schema = get_schema(r);
                        auto l_sc = RuleSet::GetCatalogAccessor()->GetSchema(std::get<1>(*l_schema.begin()));
                        auto r_sc = RuleSet::GetCatalogAccessor()->GetSchema(std::get<1>(*r_schema.begin()));
                        if(l_sc != r_sc)return false;
                        return true;
                    }break;
                    case RewriteConstrainType::C_RelEq:{
                        /**
                         * TableEq(t_x,t_y):it seems like 't' only ocurr on Input<t>
                         */
                        auto get_rel = [this](const Pattern* p) -> std::pair<catalog::db_oid_t,catalog::table_oid_t> {
                        if(p->Type() == OpType::LOGICALGET){
                                return std::make_pair(p->get_->GetDatabaseOid(),p->get_->GetTableOid());
                            }
                            return std::make_pair(catalog::INVALID_DATABASE_OID,catalog::INVALID_TABLE_OID);
                        };
                        auto l_rel = get_rel(l);
                        auto r_rel = get_rel(r);
                        if(l_rel.first != r_rel.first || l_rel.second != r_rel.second || l_rel.first != catalog::INVALID_DATABASE_OID){
                            return false;
                        }
                        return true;                        
                    }break;
                    default:{
                        return false;
                    }
                }
                return true;
            }

            bool WeTuneRule::BindPatternToPlan(common::ManagedPointer<AbstractOptimizerNode>& plan,Pattern* pattern) const {
                if(pattern == nullptr)return true;
                if(pattern->Type() != plan->Contents()->GetOpType() && pattern->Type() != OpType::LOGICALPROJECTION){
                    return false;
                }
                //if the rule root is projection,we need skip the pattern and try meet its chilid
                if(pattern->Type() == OpType::LOGICALPROJECTION){
                    pattern = pattern->Children()[0];
                }
                bool ret = true;
                for(size_t i=0;i<pattern->Children().size();i++){
                    auto child = pattern->Children()[i];
                    if(child->Type() == OpType::LOGICALPROJECTION){
                        /**
                         * 1. while meet the project pattern, we need special handling.
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
                        /**
                         * TODO: 11-05,here is a problem,if rule root pattern type is Projection, should
                         * it allowed to exist? now, we directly think it can't exist in the real plan
                         */
                    }break;
                    case OpType::LEAF:{
                        pattern->leaf_.op = plan->Contents()->GetOpType();
                        switch(pattern->leaf_.op){
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
                            case OpType::LOGICALGET:{
                                pattern->get_ = plan->Contents()->GetContentsAs<LogicalGet>();
                            }break;
                            default:{
                                std::cerr<<"leaf pattern not support this type:"<< std::to_string(int(pattern->leaf_.op))<<std::endl;
                                exit(-1);
                            }
                        }
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
                        auto left_child = BuildRewritePlan(p->Children()[0],context);
                        auto right_child = BuildRewritePlan(p->Children()[1],context);
                        c.push_back(std::move(left_child));
                        c.push_back(std::move(right_child));
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
                        auto left_child = BuildRewritePlan(p->Children()[0],context);
                        auto right_child = BuildRewritePlan(p->Children()[1],context);
                        c.push_back(std::move(left_child));
                        c.push_back(std::move(right_child));
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
                        auto left_child = BuildRewritePlan(p->Children()[0],context);
                        auto right_child = BuildRewritePlan(p->Children()[1],context);
                        c.push_back(std::move(left_child));
                        c.push_back(std::move(right_child));
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
                        auto child = BuildRewritePlan(p->Children()[0],context);
                        std::vector<std::unique_ptr<AbstractOptimizerNode>> c;
                        c.push_back(std::move(child));
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
                        auto left_child = BuildRewritePlan(p->Children()[0],context);
                        auto right_child = BuildRewritePlan(p->Children()[1],context);
                        c.push_back(std::move(left_child));
                        c.push_back(std::move(right_child));
                        return  std::make_unique<OperatorNode>(LogicalSemiJoin::Make(std::move(join_predicates))
                                                     .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
                                                 std::move(c), context->GetOptimizerContext()->GetTxn());                        
                    }break;
                    case OpType::LOGICALPROJECTION:{
                        //we should skip the project pattern,it should not occur in plan (except root node)
                        return BuildRewritePlan(p->Children()[0],context);
                    }break;
                    case OpType::LOGICALGET:{
                        std::vector<AnnotatedExpression> scan_predicates;
                        catalog::db_oid_t db_oid;
                        catalog::table_oid_t tb_oid;
                        parser::AliasType tb_alias;
                        for(const  auto &c : constrains){
                            auto l_pattern = binder_.at(c.placeholders[1]);
                            if(c.type == RewriteConstrainType::C_RelEq){
                                db_oid = l_pattern->get_->GetDatabaseOid();
                                tb_oid = l_pattern->get_->GetTableOid();
                                tb_alias = l_pattern->get_->GetTableAlias();
                            }else if(c.type == RewriteConstrainType::C_AttrsEq){
                                
                            }
                        }
                        return std::make_unique<OperatorNode>(LogicalGet::Make(db_oid,tb_oid,scan_predicates,tb_alias,false));
                    }break;
                    default:{
                        std::cerr<<"error type"<<std::endl;
                    }
                }
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
                //std::unordered_set<AnnotatedExpression,ExprHasher> l;
                ExprSet l_set;
                ExprSet r_set;
                for(auto e: l)l_set.insert(e.GetExpr());
                for(auto e: r)r_set.insert(e.GetExpr());
                for(auto l_expr: l_set){
                    if(r_set.find(l_expr) == r_set.end())return false;
                }
                return true; 
            }

            void WeTuneRule::BindProjectPattern(Pattern* p) const{
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

            std::unordered_set<std::tuple<catalog::col_oid_t,catalog::table_oid_t,catalog::db_oid_t>,TupleHash> WeTuneRule::GetJoinAttrs(std::vector<noisepage::optimizer::AnnotatedExpression>& preds, const Pattern* p,const ReWriteConstrain& c) const{
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
                        tb_oid_set.insert(tb_oid);
                    }else{
                        std::cerr<<"bad type of join children pattern"<<std::endl;
                    }
                    return tb_oid_set;
                };
                auto tb_sets = GetTableNames(p,c_pos);
                if(tb_sets.size()!=1){
                    std::cerr<<"get tb_sets for join error, tb_sets size = "<<tb_sets.size()<<std::endl;
                }
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

}