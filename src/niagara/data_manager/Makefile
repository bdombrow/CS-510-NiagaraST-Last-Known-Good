##############################################################################
#
#  Makefile to build data manager
#
#  make:             builds all
#  make install:     builds all
#                    create a 'dm.jar' file in lib dir for DM clients to link in
#                    clean class files from this dir
#             
#  make clean:       removes class files
#  make classpath:   creates and echos entire classpath
#
##############################################################################


APIDIR      = /p/niagra/api/data_manager

##############################################################################
#
#  When adding classes to the data manager, simply add your class name to this
#  list and it will be built using 'make all'
#
##############################################################################
OBJ = 	  DMCache.class         \
	  CacheUtil.class	    \
          UrlFetchThread.class      \
	  UrlQueue.class \
	  UrlQueueElement.class \
          FetchThread.class \
	  WebCache.class   \
          DiskCache.class \
          MemCacheEntry.class \
          MemCache.class \
          MemCacheFIFO.class \
          CacheManager.class \
          DTDDirectory.class \
          DTDDirEntry.class \
          DTDInfo.class \
          DTDScan.class \
          DMException.class \
          DMClosedException.class \
	  DMUtil.class \
          Mutex.class \
          DataManager.class \
          DataManager_Test.class \

.SUFFIXES: .java .class

.java.class:
	$(JAVAC) $(JAVAFLAGS) $*.java 

all: $(OBJ)

test:$(OBJ) $(TESTOBJ)

install: all
	$(JAR) -cfv $(LIBDIR)/$(TARG_LIB) *.class

doc:
	@rm -f $(APIDIR)/*.html
	$(JAVADOC) -sourcepath $(CLASSPATH) -d $(APIDIR) -bootclasspath $(BASELIB) *.java


##############################################################################
#
#  Utilities
#
##############################################################################
classpath:
	@echo $(CLASSPATH)
clean:
	@rm -f *.class *~ *.jar

cleantmp:
	@rm -rf /tmp/tmp* /tmp/.dm*
