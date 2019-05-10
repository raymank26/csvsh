grammar Sql;

parse
 : statement EOF
 ;

statement
 : select
 | createIndex
 ;

select
 : SELECT selectExpr FROM table
 | SELECT selectExpr FROM table WHERE whereExpr
 ;

table
 : IDENTIFIER_Q
 ;

selectExpr
 : selectColumn COMMA selectColumn
 | selectColumn
 ;

whereExpr
 : allIdentifiers BOOL_COMP allIdentifiers
 | whereExpr BOOLJOIN whereExpr
 ;

selectColumn
 : allIdentifiers
 | allIdentifiers LEFT_PAR allIdentifiers RIGHT_PAR
 ;

allIdentifiers
 : IDENTIFIER | IDENTIFIER_Q
 ;

createIndex
 : CREATE INDEX indexName ON table LEFT_PAR indexColumn RIGHT_PAR
 ;

indexName
 : IDENTIFIER
 ;

indexColumn
 : IDENTIFIER
 | indexColumn COMMA indexColumn
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

BOOLJOIN
 : 'AND' | 'OR'
 ;

BOOL_COMP
 : '<' | '>' | '<>' | 'LIKE'
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
 : '\''[0-9A-Za-z/]+'\''
 ;

IDENTIFIER
 : [A-Z0-9a-z]+
 ;

WHITESPACE : [ \n] -> skip ;
