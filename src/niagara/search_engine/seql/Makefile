include ../../../make.incl

all:	parsertest.class Yylex.class parser.class QueryPlan.class Optimizer.class
#	cat test.seql
#	$(JAVA) $(JAVAFLAGS) search_engine.seql.parsertest test.seql
#	dot -Tps QEP.dot > QEP.ps

sym.class: sym.java
	$(JAVAC)  $(JAVAFLAGS)  sym.java

sym.java: seql.cup
	$(JAVA) -version
	$(JAVA) $(JAVAFLAGS)  java_cup.Main -interface -package search_engine.seql < seql.cup

Yylex.java: seql.jlex
	$(JAVA) $(JAVAFLAGS) JLex.Main seql.jlex
	mv seql.jlex.java Yylex.java

parser.class: parser.java queryNode.class SyntaxErrorException.class
	$(JAVAC) $(JAVACFLAGS)  parser.java

parser.java: seql.cup
	$(JAVA) $(JAVAFLAGS)  java_cup.Main -interface -package search_engine.seql < seql.cup

SyntaxErrorException.class: SyntaxErrorException.java
	$(JAVAC) $(JAVACFLAGS)  SyntaxErrorException.java

Yylex.class: Yylex.java sym.class 
	$(JAVAC) $(JAVACFLAGS)  Yylex.java

queryNode.class: ast.java sym.class
	$(JAVAC) $(JAVACFLAGS) ast.java

QueryPlan.class: QueryPlan.java 
	$(JAVAC) $(JAVACFLAGS)  QueryPlan.java 

parsertest.class: parsertest.java Yylex.class parser.class queryNode.class
	$(JAVAC) $(JAVACFLAGS)  parsertest.java 

Optimizer.class: Optimizer.java
	$(JAVAC) $(JAVACFLAGS)  Optimizer.java 

classpath:
	@echo $(CLASSPATH)

clean:
	rm -f *.class *~
	rm -f parser.java Yylex.java  sym.java


