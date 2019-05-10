grammar Sql;

parse
 : statement EOF
 ;

statement
 : select
 ;

select
 : SELECT selectExpr FROM TABLE
 ;

selectExpr
 : column COMMA column
 | column
 ;

column
 : IDENTIFIER
 | IDENTIFIER LEFT_PAR IDENTIFIER RIGHT_PAR
 ;

COMMA
 : ','
 ;

LEFT_PAR
 : '('
 ;

RIGHT_PAR
 : ')'
 ;

IDENTIFIER
 : [A-Z0-9]+
 ;

SELECT
 : 'SELECT'
 ;

FROM
 : 'FROM'
 ;

TABLE
 : '\''[0-9A-Z/]+'\''
 ;

WHITESPACE : ' \n\r' -> skip ;
