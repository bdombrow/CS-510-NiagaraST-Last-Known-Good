
#-------------------------------------
#  Other required libs
#-------------------------------------
COMMONLIB    = $(LIBDIR)/common.jar
PARSELIB     = $(LIBDIR)/xml4j.jar
XPARSERLIB   = $(LIBDIR)/xmlql_parser.jar
REGEXPLIB    = $(LIBDIR)/gnu-regexp.jar
OPTREELIB    = $(LIBDIR)/logicaloptree.jar
CUPLIB	     = $(LIBDIR)/cup_all.jar
QELIB	     = $(LIBDIR)/qe.jar
ASTLIB	     = $(LIBDIR)/ast.jar
TRIGLIB      = $(LIBDIR)/trig.jar
DMLIB        = $(LIBDIR)/dm.jar

CLASSPATH  = .:../../data_manager/src:$(PARSELIB):$(BASELIB):$(REGEXPLIB):$(CUPLIB):$(XPARSERLIB):$(ASTLIB):$(QELIB):$(OPTREELIB):$(COMMONLIB)
JAVAFLAGS  = -classpath $(CLASSPATH) -g

OBJ = HashElementHandler.class \
      HashedElement.class \
      HashedTXText.class \
      IntSet.class \
      XMLDiff.class \
      debug.class 

.SUFFIXES: .java .class
.java.class:
	$(JAVAC) $(JAVAFLAGS) $*.java

all: $(OBJ) 

install: all
	$(JAR) -cfv $(DIFFLIB) *.class; cp diff.jar $(LIBDIR)/diff.jar

classpath: 
	@echo $(CLASSPATH)

compile: IntSet.class HashedElement.class HashedTXText.class HashElementHandler.class XMLDiff.class

clean:
	rm -f *.class
