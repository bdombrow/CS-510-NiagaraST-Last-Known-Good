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

#-------------------------------------
# Specify all relative path info here
#-------------------------------------
NIAGROOT    = ../../../..
API         = /p/niagra/api
LIBDIR      = $(NIAGROOT)/lib
TARG_LIB    = xmlql_parser.jar


#-------------------------------------
#  Other required libs
#-------------------------------------
PARSELIB    = $(LIBDIR)/xml4j.jar
COMMONLIB   = $(LIBDIR)/common.jar
CUPLIB      = $(LIBDIR)/cup_all.jar
JFLEXLIB    = $(LIBDIR)/JFlex.zip

CLASSPATH  = .:$(NIAGROOT)/src:$(BASELIB):$(JFLEXLIB):$(CUPLIB):$(PARSELIB):$(COMMONLIB)

JAVAFLAGS  = -classpath $(CLASSPATH)
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

all: sym.class Scanner.class QueryParser.class $(OBJ)

%.class: %.java
	$(JAVAC) $(JAVAFLAGS) $<

# Generate QueryParser.java and sym.java
QueryParser.java: xmlql.cup
	$(JAVA) $(JAVAFLAGS) java_cup.Main -expect 5 -parser QueryParser < xmlql.cup
sym.java: xmlql.cup
	$(JAVA) $(JAVAFLAGS) java_cup.Main -expect 5 -parser QueryParser < xmlql.cup 

# Generate the scanner
Scanner.java: xmlql.flex sym.class
	$(JAVA) $(JAVAFLAGS) JFlex.Main xmlql.flex



install: sym.class Scanner.class QueryParser.class $(OBJ)
	cd ../../..;$(JAR) -cfv ../lib/$(TARG_LIB) niagara/xmlql_parser/syntax_tree/*.class


#####################################################################

classpath:
	@echo $(CLASSPATH)

clean:
	rm -f *.class *~
	rm -f QueryParser.java sym.java Scanner.java

