############################################################################
#
#  Usage
#  ------------------------------------------------------------
#  make:          builds all class files
#  make clean:    deletes all class files and generated files
#
############################################################################

include ../../../make.incl

SRCS = DocMap.java \
	InvertedFile.java \
	FlatInvertedFile.java \
	PagedInvertedFile.java \
	IMException.java \
	IMFileNotFoundException.java \
	IMIVLFileException.java \
	IMPositionException.java \
	IMURLLenException.java \
	IVLEntry.java \
	IVL.java \
	IVLMgr.java \
	IndexMgr.java \
	IndexMgrTest.java \
	IndexParser.java \
	LEFrequencyComparator.java \
	Lexicon.java \
	LexiconEntry.java \
	TMLexicon.java \
	NumberLexicon.java \
	Position.java \
	StopWord.java \
	WordEntry.java \
	WordReaderException.java \
	Util.java 

OBJS = $(SRCS:.java=.class)

###############################################################################

all:  $(OBJS) 

classpath:
	@echo $(CLASSPATH)

run: all
	$(JAVA) -mx1500M niagara.search_engine.indexmgr.IndexMgrTest

clean:
	rm -f *.class *~

new: all
	rm -rf seivl/*
