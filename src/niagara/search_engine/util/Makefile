############################################################################
#
#  Usage
#  ------------------------------------------------------------
#  make:          builds all class files
#  make clean:    deletes all class files and generated files
#
############################################################################

include ../make.incl

#############################################################################
SRCS = 	Const.java 			\
	Tuple.java			\
	Header.java                 	\
	Pair.java                   	\
	Table.java

OBJS = $(SRCS:.java=.class)

###############################################################################

all:  $(OBJS) 

#####################################################################

install: all
	$(JAR) -cfv $(TARG_LIB) *.class
	cp -f $(TARG_LIB) $(LIBDIR)

classpath:
	@echo $(CLASSPATH)

clean:
	rm -f *.class *~

