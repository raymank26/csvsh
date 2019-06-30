grammar Sql;

parse
 : statement EOF
 ;

statement
 : select
 | describeSelect
 | createIndex
 | dropIndex
 | describeTable
 ;

select
 : SELECT selectExpr FROM table (GROUP_BY groupByExpr)? (WHERE whereExpr)? (ORDER_BY orderByExpr)? (LIMIT limitExpr)?
 ;

describeSelect
 : DESCRIBE select
 ;

groupByExpr
 : reference (COMMA reference)*
 ;

orderByExpr
 : selectColumn DESC?
 ;

limitExpr
 : INTEGER
 ;

table
 : IDENTIFIER_Q
 ;

selectExpr
 : selectColumn (COMMA selectColumn)*
 | allColumns
 ;

allColumns
 : STAR
 ;

whereExpr
 : variable BOOL_COMP variable #whereExprAtom
 | variable BOOL_COMP_IN LEFT_PAR variable (COMMA variable)* RIGHT_PAR #whereExprIn
 | LEFT_PAR whereExpr RIGHT_PAR #whereExprPar
 | whereExpr (AND | OR) whereExpr #whereExprBool
 ;

selectColumn
 : AGG LEFT_PAR reference RIGHT_PAR #selectColumnAgg
 | reference #selectColumnPlain
 ;

variable locals [String type]
 : integerNumber { $type = "integer"; }
 | floatNumber { $type = "float"; }
 | string { $type = "string"; }
 | reference { $type = "reference"; }
 ;

reference
 : IDENTIFIER
 ;

string
 : IDENTIFIER_Q
 ;

integerNumber
 : INTEGER
 ;

floatNumber
 : FLOAT
 ;

createIndex
 : CREATE INDEX indexName ON table LEFT_PAR reference RIGHT_PAR
 ;

dropIndex
 : DROP INDEX indexName ON table
 ;

describeTable
 : DESCRIBE TABLE table
 ;

indexName
 : IDENTIFIER
 ;

INTEGER
 : [0-9]+
 | '-' INTEGER
 ;

fragment EXPONENT : ('e'|'E') ('+'|'-')? ('0'..'9')+ ;

FLOAT
 :   ('0'..'9')+ '.' ('0'..'9')* EXPONENT?
 |   '.' ('0'..'9')+ EXPONENT?
 |   ('0'..'9')+ EXPONENT
 | '-' FLOAT
 ;

ON
 : 'ON'
 ;

COMMA
 : ','
 ;

LEFT_PAR
 : '('
 ;

AND
 : 'AND'
 ;

OR
 : 'OR'
 ;

STAR
 : '*'
 ;

BOOL_COMP
 : '<' | '<=' | '>' | '>=' | '<>' | 'LIKE' | '=' | 'NOT LIKE'
 ;

AGG
 : 'MAX' | 'MIN' | 'SUM' | 'COUNT'
 ;

BOOL_COMP_IN
 : 'NOT IN' | 'IN'
 ;

RIGHT_PAR
 : ')'
 ;

SELECT
 : 'SELECT'
 ;

WHERE
 : 'WHERE'
 ;

ORDER_BY
 : 'ORDER BY'
 ;

DESC
 : 'DESC'
 ;

GROUP_BY
 : 'GROUP BY'
 ;

LIMIT
 : 'LIMIT'
 ;

FROM
 : 'FROM'
 ;

CREATE
 : 'CREATE'
 ;

DROP
 : 'DROP'
 ;

DESCRIBE
 : 'DESCRIBE'
 ;

TABLE
 : 'TABLE'
 ;

INDEX
 : 'INDEX'
 ;

IDENTIFIER_Q
 : '\'' IDENTIFIER '\''
 ;

IDENTIFIER
 : [A-Z0-9a-zА-Яа-я./*%_\-]+
 ;

WHITESPACE : [ \n] -> skip ;
