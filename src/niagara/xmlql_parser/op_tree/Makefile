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

include ../../../make.incl

#-------------------------------------
# Specify all relative path info here
#-------------------------------------
TARG_LIB    = logicaloptree.jar


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
	dtdScanOp.class		\
	scanOp.class		\
	selectOp.class		\
	joinOp.class		\
	constructOp.class	\
	groupOp.class		\
	nestOp.class		\
	averageOp.class		\
	logNode.class		\
	logPlanGenerator.class

##############################################################################

%.class: %.java
	$(JAVAC) $(JAVACFLAGS) $<

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






