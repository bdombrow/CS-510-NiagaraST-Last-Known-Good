include ../make.incl

all:	parsertest.class Yylex.class parser.class QueryPlan.class Optimizer.class
#	cat test.seql
#	$(JAVA) $(JAVAFLAGS) search_engine.seql.parsertest test.seql
#	dot -Tps QEP.dot > QEP.ps

sym.class: sym.java
	$(JAVAC)  $(JAVAFLAGS)  sym.java

sym.java: seql.cup
	$(JAVA) $(JAVAFLAGS)  java_cup.Main -interface -package search_engine.seql < seql.cup

Yylex.java: seql.jlex
	$(JAVA) $(JAVAFLAGS) JLex.Main seql.jlex
	mv seql.jlex.java Yylex.java

parser.class: parser.java queryNode.class SyntaxErrorException.class
	$(JAVAC) $(JAVAFLAGS)  parser.java

parser.java: seql.cup
	$(JAVA) $(JAVAFLAGS)  java_cup.Main -interface -package search_engine.seql < seql.cup

SyntaxErrorException.class: SyntaxErrorException.java
	$(JAVAC) $(JAVAFLAGS)  SyntaxErrorException.java

Yylex.class: Yylex.java sym.class 
	$(JAVAC) $(JAVAFLAGS)  Yylex.java

queryNode.class: ast.java sym.class
	$(JAVAC) $(JAVAFLAGS) ast.java

QueryPlan.class: QueryPlan.java 
	$(JAVAC) $(JAVAFLAGS)  QueryPlan.java 

parsertest.class: parsertest.java Yylex.class parser.class queryNode.class
	$(JAVAC) $(JAVAFLAGS)  parsertest.java 

Optimizer.class: Optimizer.java
	$(JAVAC) $(JAVAFLAGS)  Optimizer.java 

classpath:
	@echo $(CLASSPATH)

clean:
	rm -f *.class *~
	rm -f parser.java Yylex.java  sym.java


