############################################################################
#
#  Usage
#  ------------------------------------------------------------
#  make:          builds all class files
#  make clean:    deletes all class files and generated files
#
############################################################################

include ../../../make.incl

#############################################################################

SRCS =  AbstractOperator.java \
	ANDOp.java \
	ContainOp.java \
	ContainedOp.java \
	DistanceOp.java \
	EXCEPTOp.java \
	ISOp.java \
	IVLOp.java \
	OrderOp.java \
	OROp.java \
	StringOp.java \
	ConformstoOp.java \
	DTDOp.java \
	ElementOp.java \
	NotContainOp.java \
	MergeOp.java \
	TestOps.java 


OBJS = $(SRCS:.java=.class)

###############################################################################

all:  $(OBJS) 

install: all
	$(JAR) -cfv $(TARG_LIB) *.class
	cp -f $(TARG_LIB) $(LIBDIR)

classpath:
	@echo $(CLASSPATH)

clean:
	rm -f *.class *~

