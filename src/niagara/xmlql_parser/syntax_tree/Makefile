############################################################################
#
# Build the XML-QL parser
#
#  Usage
#  ------------------------------------------------------------
#  make:          builds all class files
#  make install:  re-builds parser jar file in the installed lib dir
#  make clean:    deletes all class files
#
############################################################################
include ../../../make.incl

API         = /p/niagra/api
TARG_LIB    = xmlql_parser.jar

#############################################################################


OBJ = EscapedUnicodeReader.class   \
      Util.class		   \
      Schema.class		   \
      SchemaUnit.class		   \
      Lexer.class		   \
      arg.class                    \
      attr.class                   \
      schemaAttribute.class	   \
      condition.class              \
      set.class			   \
      inClause.class		   \
      data.class                   \
      constructBaseNode.class      \
      constructLeafNode.class      \
      constructInternalNode.class  \
      opType.class                 \
      dataType.class		   \
      varType.class		   \
      pattern.class                \
      patternInternalNode.class    \
      patternLeafNode.class        \
      pnode.class		   \
      predicate.class              \
      predArithOpNode.class	   \
      predLogOpNode.class	   \
      query.class                  \
      regExp.class           	   \
      regExpOpNode.class           \
      regExpDataNode.class         \
      varTbl.class		   \
      varToAttr.class		   \
      skolem.class                 \
      startTag.class               \
      stp.class                    \
      xqlExt.class                 \

###############################################################################

all: sym.class Scanner.class ConstructParser.class REParser.class QueryParser.class $(OBJ)

%.class: %.java
	$(JAVAC) $(JAVACFLAGS) $<

# Generate QueryParser.java and sym.java
QueryParser.java: xmlql.cup
	$(JAVA) $(JAVAFLAGS) java_cup.Main -expect 5 -parser QueryParser < xmlql.cup
sym.java: xmlql.cup
	$(JAVA) $(JAVAFLAGS) java_cup.Main -expect 5 -parser QueryParser < xmlql.cup 

# Generate the scanner
Scanner.java: xmlql.flex sym.class
	$(JAVA) $(JAVAFLAGS) JFlex.Main xmlql.flex

#Specialized parsers for paths and constructs
re.cup: xmlql.cup
	sed -e 's!start with XmlqlExt!start with RegularExpression!' -e 's!QueryParser!REParser!g' < xmlql.cup > re.cup

construct.cup: xmlql.cup
	sed -e 's!start with XmlqlExt!start with QueryInConstruct!' -e 's!QueryParser!ConstructParser!g' < xmlql.cup > construct.cup

# Generate REParser.java 
REParser.java: re.cup
	$(JAVA) $(JAVAFLAGS) java_cup.Main -expect 5 -parser REParser -symbols RESym -package niagara.xmlql_parser.syntax_tree < re.cup 

# Generate ConstructParser.java 
ConstructParser.java: construct.cup
	$(JAVA) $(JAVAFLAGS) java_cup.Main -expect 5 -parser ConstructParser -symbols ConstructSym -package niagara.xmlql_parser.syntax_tree < construct.cup 


install: sym.class Scanner.class re_parser/REParser.class construct_parser/ConstructParser.class QueryParser.class $(OBJ)
	cd ../../..;$(JAR) -cfv ../lib/$(TARG_LIB) niagara/xmlql_parser/syntax_tree/*.class


#####################################################################

classpath:
	@echo $(CLASSPATH)

clean:
	rm -f *.class *~ re.cup construct.cup REParser.java RESym.java ConstructParser.java ConstructSym.java

