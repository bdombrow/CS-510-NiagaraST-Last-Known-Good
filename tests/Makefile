export NIAGARA_ROOT = ..
export LIB = ${NIAGARA_ROOT}/lib
export SRC = ${NIAGARA_ROOT}/src

export FIREHOSE_ROOT = ../../firehose_1_0

export CLASSPATH = ${FIREHOSE_ROOT}:${SRC}:${LIB}/gnu-regexp.jar:${LIB}/xml4j.jar:${LIB}/aelfred.jar:${LIB}/java_cup.jar:${LIB}/JLex.jar:${LIB}/cup_runtime.jar:${LIB}/diva.jar:${LIB}/djava.jar:${LIB}/htmllayout.jar:${LIB}/com.mortbay.jetty.jar:${LIB}/javax.servlet.jar:${LIB}/xerces.jar:${LIB}/junit.jar:.

%.class : %.java
	javac -classpath ${CLASSPATH} $<

tests: ndom

ndom: TestNDOM.class
	@echo "Testing sanity of DOM implementations"
	@java -classpath ${CLASSPATH} TestNDOM

classpath:
	@echo ${CLASSPATH}
