#######################################################################
#
#  Makefile to build query engine related stuff
#
#  make:             builds all
#  make install:     builds all
#                    create a 'qe.jar' file in lib dir for QE clients to link in
#                    clean class files from this dir
#             
#  make clean:       removes class files
#  make classpath:   creates and echos entire classpath
#
########################################################################


#-------------------------------------
# Specify all relative path info here
#-------------------------------------
export NIAGROOT    = ..
export JAVA_HOME   = 
export LIBDIR      = $(NIAGROOT)/lib
export FIREHOSEDIR = ../../firehose_1_0

#-------------------------------------
#  Specify required libs here
#-------------------------------------
export JAVAC       = $(JAVA_HOME)javac
export JAVA        = $(JAVA_HOME)java
export JAR         = $(JAVA_HOME)jar
export JAVADOC     = $(JAVA_HOME)javadoc

#----------------
#  Niagra libs
#----------------

export XML4J          = $(LIBDIR)/xml4j.jar
#export OLDPARSELIB   = $(LIBDIR)/xml4j.old.jar
export COMMONLIB     = $(LIBDIR)/common.jar
export XMLQLPARSE    = $(LIBDIR)/xmlql_parser.jar
export LOGICALOPTREE = $(LIBDIR)/logicaloptree.jar
export CUPLIB        = $(LIBDIR)/cup_runtime.jar:$(LIBDIR)/cup_all.jar:$(LIBDIR)/java_cup.jar
export REGEXPLIB     = $(LIBDIR)/gnu-regexp.jar
export SWINGLIB      = $(LIBDIR)/swingall.jar
export QELIB         = $(LIBDIR)/qe.jar
export DMLIB         = $(LIBDIR)/dm.jar
export QBELIB1	      = $(LIBDIR)/qbe1.jar
export TRIGLIB	      = $(LIBDIR)/trig.jar
export QBELIB2	      = $(LIBDIR)/qbe2.jar
export SAX	      = $(LIBDIR)/aelfred.jar
export DIFF	      	= $(LIBDIR)/diff.jar
export JFLEX		= $(LIBDIR)/JFlex.zip:$(LIBDIR)/JLex.jar
export FHOSE            = $(FIREHOSEDIR)
export DIVA	    = $(LIBDIR)/diva.jar
export DJAVA	    = $(LIBDIR)/djava.jar
export HTMLLAYOUT   = $(LIBDIR)/htmllayout.jar
export JETTY	    = $(LIBDIR)/com.mortbay.jetty.jar
export SERVLET	    = $(LIBDIR)/javax.servlet.jar

export XERCES	    = $(LIBDIR)/xerces.1.4.2.jar

export CLASSPATH     = .:$(XERCES):$(COMMONLIB):$(REGEXPLIB):$(XMLQLPARSE):$(LOGICALOPTREE):$(DMLIB):$(CUPLIB):$(XML4J):$(TRIGGERM):$(TRIGLIB):$(QBELIB2):$(SAX):$(DIFF):$(FHOSE):$(DIVA):$(DJAVA):$(HTMLLAYOUT):$(JETTY):$(SERVLET)

export DEBUG = -g

export JAVAFLAGS     = -classpath $(CLASSPATH) $(DEBUG)

# local stuff
APPLET_DIR = /p/niagara/public/html


##############################################################################
#
#  When adding classes to the query engine, simply add your class name to this
#  list and it will be built using 'make all'
#
##############################################################################
all:	se_server ndom ql_parser con_server qe trigger client mqpclient dm applet type_system saxdom

#client: 
#	$(JAVAC) $(JAVAFLAGS) `find niagara/client -name \*.java`
client:
	$(JAVAC) $(JAVAFLAGS) niagara/client/NiagaraUI.java niagara/client/SimpleClient.java niagara/client/qpclient/*.java

mqpclient:
	$(JAVAC) $(JAVAFLAGS) niagara/client/MQPClient.java niagara/client/DisplayServlet.java 

con_server:
	$(JAVAC) $(JAVAFLAGS) niagara/connection_server/*.java

ql_parser:
	cd niagara/xmlql_parser/syntax_tree && $(MAKE)
	cd niagara/xmlql_parser/op_tree && $(MAKE)

trigger:
	$(JAVAC) $(JAVAFLAGS) niagara/trigger_engine/*.java
qe:
	$(JAVAC) $(JAVAFLAGS) niagara/query_engine/*.java
dm:
	$(JAVAC) $(JAVAFLAGS) niagara/data_manager/*.java

nitree:
	$(JAVAC) $(JAVAFLAGS) niagara/utils/nitree/NIDocument.java

ndom:
	$(JAVAC) $(JAVAFLAGS) niagara/ndom/*.java

saxdom:
	$(JAVAC) $(JAVAFLAGS) niagara/ndom/saxdom/*.java

type_system:
	$(JAVAC) $(JAVAFLAGS) niagara/utils/type_system/*.java

se_server:
	cd niagara/search_engine && $(MAKE)


applet: 
	$(JAVAC) NiagaraUIApplet.java AppletSocketRouter.java; \
	$(JAR) xf $(LIBDIR)/aelfred.jar; \
	$(JAR) cmf niagara/client/mnf niagara.jar \
						`find niagara/client -name \*.class`\
						`find niagara/client -name \*.gif`\
						`find com -name \*.class`; \
	rm -r com META-INF

applet_install:
	cp *.class niagara.jar $(APPLET_DIR)

applet_clean:
	rm *.class niagara.jar



##############################################################################
#
#  Utilities
#
##############################################################################
classpath:
	@echo $(CLASSPATH)
clean:
	rm -f */*.class */*/*.class */*/*/*.class */*/*/*/*.class *.class niagara.jar

really_clean:
	rm -f */*.class */*/*.class */*/*/*.class */*/*/*/*.class *.class niagara.jar \
	niagara/xmlql_parser/syntax_tree/QueryParser.java \
	niagara/xmlql_parser/syntax_tree/sym.java \
	niagara/xmlql_parser/syntax_tree/Scanner.java \
	niagara/search_engine/seql/parser.java \
	niagara/search_engine/seql/Yylex.java \
        niagara/search_engine/seql/sym.java
