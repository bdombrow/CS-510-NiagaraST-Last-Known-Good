SE_SRC_DIRS = util seql indexmgr operators server 

all:
	@for dir in ${SE_SRC_DIRS};\
	do cd $${dir}; \
	   make ; \
	   cd ..; \
	done

clean:
	@for dir in ${SE_SRC_DIRS};\
	do cd $${dir};\
	   make clean; \
	   cd ..; \
	done
