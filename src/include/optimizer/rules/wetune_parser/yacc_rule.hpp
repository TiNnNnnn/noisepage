/* A Bison parser, made by GNU Bison 3.5.1.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2020 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* Undocumented macros, especially those whose name start with YY_,
   are private implementation details.  Do not rely on them.  */

#ifndef YY_YY_YACC_RULE_HPP_INCLUDED
# define YY_YY_YACC_RULE_HPP_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int yydebug;
#endif

/* Token type.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    SEMICOLON = 258,
    BY = 259,
    CREATE = 260,
    DROP = 261,
    GROUP = 262,
    TABLE = 263,
    TABLES = 264,
    INDEX = 265,
    CALC = 266,
    SELECT = 267,
    DESC = 268,
    SHOW = 269,
    SYNC = 270,
    INSERT = 271,
    DELETE = 272,
    UPDATE = 273,
    LBRACE = 274,
    RBRACE = 275,
    LBRACKET = 276,
    RBRACKET = 277,
    COMMA = 278,
    TRX_BEGIN = 279,
    TRX_COMMIT = 280,
    TRX_ROLLBACK = 281,
    INT_T = 282,
    STRING_T = 283,
    FLOAT_T = 284,
    DATE_T = 285,
    VECTOR_T = 286,
    HELP = 287,
    EXIT = 288,
    DOT = 289,
    INTO = 290,
    VALUES = 291,
    FROM = 292,
    WHERE = 293,
    AND = 294,
    SET = 295,
    ON = 296,
    LOAD = 297,
    INFILE = 298,
    EXPLAIN = 299,
    STORAGE = 300,
    FORMAT = 301,
    DATA = 302,
    EQ = 303,
    LT = 304,
    GT = 305,
    LE = 306,
    GE = 307,
    NE = 308,
    LIKE = 309,
    L2_DISTANCE = 310,
    COSINE_DISTANCE = 311,
    INNER_DISTANCE = 312,
    INNER = 313,
    JOIN = 314,
    IN = 315,
    EXSIST = 316,
    NOT = 317,
    UNIQUE = 318,
    NULLABLE = 319,
    PRIMARY = 320,
    KEY = 321,
    BAR = 322,
    RELEQ = 323,
    ID = 324
  };
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 113 "yacc_rule.y"

  ParsedSqlNode *sql_node;
  Pattern* pattern;
  ReWriteConstrain * cs;
  std::vector<ReWriteConstrain> cs_list;
  std::string string;

#line 135 "yacc_rule.hpp"

};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif

/* Location type.  */
#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE YYLTYPE;
struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
};
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif



int yyparse (const char * sql_string, ParsedSqlResult * sql_result, void * scanner);

#endif /* !YY_YY_YACC_RULE_HPP_INCLUDED  */
