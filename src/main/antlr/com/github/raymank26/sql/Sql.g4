grammar Sql;

parse
 : statement EOF
 ;

statement
 : select
 | createIndex
 ;

select
 : SELECT selectExpr FROM table (WHERE whereExpr)? (ORDER_BY orderByExpr)? (LIMIT limitExpr)?
 ;

orderByExpr
 : reference
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
 : variable
 | variable LEFT_PAR variable RIGHT_PAR
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
 : (UNARY_MINUS)? INTEGER
 ;

floatNumber
 : (UNARY_MINUS)? FLOAT
 ;

createIndex
 : CREATE INDEX indexName ON table LEFT_PAR reference RIGHT_PAR
 ;

indexName
 : IDENTIFIER
 ;

INTEGER
 : [0-5]+
 ;

fragment EXPONENT : ('e'|'E') ('+'|'-')? ('0'..'9')+ ;

FLOAT
 :   ('0'..'9')+ '.' ('0'..'9')* EXPONENT?
 |   '.' ('0'..'9')+ EXPONENT?
 |   ('0'..'9')+ EXPONENT
 ;

UNARY_MINUS
 : '-'
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
 : '<' | '>' | '<>' | 'LIKE' | '='
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

LIMIT
 : 'LIMIT'
 ;

FROM
 : 'FROM'
 ;

CREATE
 : 'CREATE'
 ;

INDEX
 : 'INDEX'
 ;

IDENTIFIER_Q
 : '\'' IDENTIFIER '\''
 ;

IDENTIFIER
 : [A-Z0-9a-z./]+
 ;

WHITESPACE : [ \n] -> skip ;
