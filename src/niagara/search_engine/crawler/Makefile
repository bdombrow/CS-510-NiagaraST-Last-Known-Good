#######################################################################
#
# Usage
# -------------------------------------------------------------------
# make: builds all class files
# make clean: deletes all class files and generated files.
#
#######################################################################

include ../make.incl

NIAGARALIB = ../../../../lib

WRAPPERLIB = $(NIAGARALIB)/w4f.jar:$(NIAGARALIB)/pat132.zip

CLASSPATH = .:../../..:$(SEHOME):$(NIAGARALIB)/xml4j.jar:$(NIAGARALIB)/aelfred.jar:$(NIAGARALIB)/JLex.jar:$(NIAGARALIB)/PerlTools.jar:$(WRAPPERLIB)

JAVAFLAGS = -classpath $(CLASSPATH)

SRCS = URLQueue.java\
	StorageManager.java\
	Crawler.java\
	SECrawler.java\
	IMDBCrawler.java\
	BMStorageManager.java\
	BMCrawler.java\
	clearBM.java\

OBJS = $(SRCS:.java=.class)

#########################################################################

all: $(OBJS)

classpath:
	@echo $(CLASSPATH)

clean:
	rm -f *.class *~

