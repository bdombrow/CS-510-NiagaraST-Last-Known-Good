/*
README

	Columbia Optimizer Framework

	A Joint Research Project of Portland State University 
	   and the Oregon Graduate Institute
	Directed by Leonard Shapiro and David Maier
	Supported by NSF Grants IRI-9610013 and IRI-9619977
*/

Columbia is a successor to the Cascades Query Optimizer.  It will be a
testbed for investigating various algorithms, rule sets, operators,
workloads, etc.

Here is a top-down ordering of the specification files.  Details are in the
files themselves.

  query.h   for reading in the inital query: QUERY, EXPR
  cat.h     catalog.  Info about stored data: CAT
  ssp.h     Search space SSP, GROUP, M_EXPR, WINNER.  
  op.h      ABC classes for any operator, plus logical, physical, and item
			operators.  Also leaf class, which is not ABC. 
  logop.h   logical operators: GET, SELECT, PROJECT, etc.
  physop.h  physical operators: FILE_SCAN, P_SELECT, LOOPS_JOIN, etc.
  item.h 	item operators: ATTR_OP, CONST_INT_OP, etc.
  rules.h   RULE (element is a single rule), RULE_SET (one element, contains
			a vector of all active rules), BINDERY (creates bindings between
			subexpressions and patterns), actual rules (e.g., GET_TO_FILE_SCAN).
  defs.h    #includes, #defines, typedefs, tracing.  Programming support
  supp.h    supplementary classes, used throughout, optimization support
			Keys, properties of stored objects, attribute, schema,
			logical and physical properties of stored and passed objects,
			context of a search, cost
  cm.h		Cost model, class CM.  Cost of copying a tuple, etc.
  tasks.h   TASKS (ABC for tasks), individual tasks (e.g., O_GROUP),
			PTASKS (stack of pending tasks.)
  bm.h	Bill McKenna's memory management code.
  wcol.h    MFC windows stuff: app, frame, doc, view.
            a window framework for displaying optimizer's trace output

The symbol XX in the code means more work may be needed.

Stdafx.h is used as the precompiled header.  Thus it contains .h files which change very little, and system files:
Wcol.h; defs.h; bm.h; supp.h; <math.h>

#include <math.h>

Each of these #includes the file above it, as shown by lines


     defs.h   bm.h
		  \  /
           \/
         supp.h-------------
           |          |    |
         -op.h--	cm.h cat.h
	   	/   |	\
	   /	|    \
 phys.h  logop.h item.h
            |	 /			
          query.h
            |
           ssp.h 
            |
          rules.h
            |
          tasks.h

Some assumptions we have made:

1) Cascades source code placed the responsiblitiies of the DBI in
separate source files, to facilitate DBI extensibility.  This made
development more complex since the base class was often in a different
file than the derived class. Columbia will decide file membership
based on what seems natural, e.g. placing derived classes with base
classes where appropriate.

2) Sharing will be done for conditions only.  It is our estimate that
condition is the only shared object which has potentially many aliases
per object.  Also, sharing conditions has the added benefit of sharing
the information about when a search is done.  Thus all copy constructors
and opertor='s will be deep copies, except in the COND class.  Sharing is
nontrivial because simple sharing schemes require monitoring the copying
of pointers.


