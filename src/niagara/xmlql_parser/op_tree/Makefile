##############################################################################
#
#  Makefile to build plan generator
#
#  make:             builds all
#             
#  make clean:       removes class files
#  make classpath:   creates and echos entire classpath
#
##############################################################################


#-------------------------------------
# Specify all relative path info here
#-------------------------------------
NIAGROOT    = ../../../..
API         = /p/niagra/api
LIBDIR      = $(NIAGROOT)/lib
TARG_LIB    = logicaloptree.jar


#-------------------------------------
#  Other required libs
#-------------------------------------
PARSELIB     = $(LIBDIR)/xml4j.jar
COMMONLIB    = $(LIBDIR)/common.jar
CUPLIB	     = $(LIBDIR)/cup_all.jar
ASTLIB	     = $(LIBDIR)/ast.jar
REGEXPLIB     = $(LIBDIR)/gnu-regexp.jar
JETTY        = $(LIBDIR)/com.mortbay.jetty.jar
SERVLET      = $(LIBDIR)/javax.servlet.jar
export SAX   = $(LIBDIR)/aelfred.jar


CLASSPATH  = .:$(NIAGROOT)/src:$(PARSELIB):$(BASELIB):$(CUPLIB):$(ASTLIB):$(COMMONLIB):$(NIAGROOT)/src/$(FHOSE):$(REGEXPLIB):$(JETTY):$(SERVLET):$(SAX)
JAVAFLAGS  = -classpath $(CLASSPATH)


##############################################################################
#
#  When adding classes to the query engine, simply add your class name to this
#  list and it will be built using 'make all'
#
##############################################################################
OBJ =   op.class		\
	unryOp.class		\
	binOp.class		\
	dupOp.class		\
	splitOp.class		\
	dbScanOp.class		\
	dtdScanOp.class		\
	tupleScanOp.class	\
	scanOp.class		\
	selectOp.class		\
	joinOp.class		\
	constructOp.class	\
	groupOp.class		\
	nestOp.class		\
	averageOp.class		\
	operators.class		\
	logNode.class		\
	logPlanGenerator.class  \
        trigActionOp.class

##############################################################################

%.class: %.java
	$(JAVAC) $(JAVAFLAGS) $<

all: $(OBJ) 

install: all
	cd ../../..;$(JAR) -cfv ../lib/$(TARG_LIB) niagara/xmlql_parser/op_tree/*.class

##############################################################################
#
#  Utilities
#
##############################################################################
classpath:
	@echo $(CLASSPATH)
clean:
	@rm -f *.class *~ *.jar






