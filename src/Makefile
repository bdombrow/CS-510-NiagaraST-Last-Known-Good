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

include make.incl

##############################################################################
#
#  When adding classes to the query engine, simply add your class name to this
#  list and it will be built using 'make all'
#
##############################################################################
all:	se_server ndom ql_parser con_server qe trigger client mqpclient dm applet type_system saxdom

client:
	$(JAVAC) $(JAVAFLAGS) niagara/client/*.java niagara/client/qpclient/*.java

#$(JAVAC) $(JAVACFLAGS) niagara/client/NiagaraUI.java niagara/client/SimpleClient.java niagara/client/qpclient/*.java

mqpclient:
	$(JAVAC) $(JAVACFLAGS) niagara/client/MQPClient.java niagara/client/DisplayServlet.java 

con_server:
	$(JAVAC) $(JAVACFLAGS) niagara/connection_server/*.java

ql_parser:
	cd niagara/xmlql_parser/syntax_tree && $(MAKE)
	cd niagara/xmlql_parser/op_tree && $(MAKE)

trigger:
	$(JAVAC) $(JAVACFLAGS) niagara/trigger_engine/*.java
qe:
	$(JAVAC) $(JAVACFLAGS) niagara/query_engine/*.java
dm:
	$(JAVAC) $(JAVACFLAGS) niagara/data_manager/*.java

#nitree:
#	$(JAVAC) $(JAVACFLAGS) niagara/utils/nitree/NIDocument.java

ndom:
	$(JAVAC) $(JAVACFLAGS) niagara/ndom/*.java

saxdom:
	$(JAVAC) $(JAVACFLAGS) niagara/ndom/saxdom/*.java

type_system:
	$(JAVAC) $(JAVACFLAGS) niagara/utils/type_system/*.java

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
	rm -f */*~ */*/*~ */*/*/*~ */*/*/*/*~ *~ niagara.jar
	cd niagara/xmlql_parser/syntax_tree && make clean
	cd niagara/xmlql_parser/op_tree && make clean 
	cd niagara/search_engine && make clean 

