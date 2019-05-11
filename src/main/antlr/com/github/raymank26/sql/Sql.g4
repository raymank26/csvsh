grammar Sql;

parse
 : statement EOF
 ;

statement
 : select
 | createIndex
 ;

select
 : SELECT selectExpr FROM table (WHERE whereExpr)?
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
 : variable BOOL_COMP variable
 | LEFT_PAR whereExpr RIGHT_PAR
 | whereExpr AND whereExpr
 | whereExpr OR whereExpr
 ;

selectColumn
 : variable
 | variable LEFT_PAR variable RIGHT_PAR
 ;

variable
 : LEFT_PAR variable (COMMA variable)* RIGHT_PAR
 | integerNumber
 | floatNumber
 | string
 | reference
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
 : CREATE INDEX indexName ON table LEFT_PAR indexColumn RIGHT_PAR
 ;

indexName
 : IDENTIFIER
 ;

indexColumn
 : indexColumnExpr (COMMA indexColumnExpr)*
 ;

indexColumnExpr
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
 : '<' | '>' | '<>' | 'LIKE' | '=' | 'IN'
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
