#!/bin/bash
flex --outfile lex_rule.cpp --header-file=lex_rule.h lex_rule.l
`which bison` -d -v --output yacc_rule.cpp yacc_rule.y