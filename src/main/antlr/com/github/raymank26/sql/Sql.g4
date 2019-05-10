grammar Sql;

parse
 : statement EOF
 ;

statement
 : select
 ;

select
 : SELECT selectExpr FROM TABLE
 | SELECT selectExpr FROM TABLE WHERE whereExpr
 ;

selectExpr
 : selectColumn COMMA selectColumn
 | selectColumn
 ;

whereExpr
 : IDENTIFIER BOOL_COMP IDENTIFIER
 | whereExpr BOOLJOIN whereExpr
 ;

selectColumn
 : IDENTIFIER
 | IDENTIFIER LEFT_PAR IDENTIFIER RIGHT_PAR
 ;

COMMA
 : ','
 ;

LEFT_PAR
 : '('
 ;

BOOLJOIN
 : 'AND' | 'OR'
 ;

BOOL_COMP
 : '<' | '>' | '<>'
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

TABLE
 : '\''[0-9A-Za-b/]+'\''
 ;

IDENTIFIER
 : [A-Z0-9a-b]+
 ;

WHITESPACE : [ \n] -> skip ;
