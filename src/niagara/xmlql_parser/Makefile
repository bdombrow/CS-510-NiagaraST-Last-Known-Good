SRC_DIRS = syntax_tree op_tree

all:
	@for dir in ${SRC_DIRS};\
	do cd $${dir}; \
	   make ; \
	   cd ..; \
	done

clean:
	@for dir in ${SRC_DIRS};\
	do cd $${dir};\
	   make clean; \
	   cd ..; \
	done
