include ../src/make.incl

tests: ndom catalog

ndom: TestNDOM.class
	@echo "Testing sanity of DOM implementations"
	@java -classpath ${CLASSPATH} TestNDOM

catalog: TestCatalog.class
	@echo "Testing sanity of catalog parsing"
	@java -classpath ${CLASSPATH} TestCatalog

classpath : FORCE
	@echo ${CLASSPATH}

%.class : %.java
	@javac -classpath ${CLASSPATH} $<


FORCE : 