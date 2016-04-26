/*
 * btree_mgr_assn.h
 *
 *      Author: Smriti
 */

#ifndef ASSIGN4_ASSIGN4_BTREE_MGR_ASSN_H_
#define ASSIGN4_ASSIGN4_BTREE_MGR_ASSN_H_
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<math.h>
#include<unistd.h>
#include"storage_mgr.h"
#include"buffer_mgr.h"
#include"record_mgr.h"
#include"btree_mgr.h"
#include"tables.h"
#include"dberror.h"


typedef struct nodeData{

		struct nodeData *next;
		int value;

}nodeData;

typedef struct nodesInBtree{

		struct nodesInBtree *leftchild;
		struct nodesInBtree *rightchild;
		struct nodesInBtree *middleChild;
		struct nodeData *data;
		RID id;
		int totalValuesInNode;
		int page;
		int slot;
		int begin;
		int end;

}nodesInBtree;

typedef struct btreeInfoNodes{

		nodesInBtree *rootNode;
		int totalNode;
		int page;
				int slot;
				int begin;
				int end;

}btreeInfoNodes;

typedef struct bTreeManager{

		nodesInBtree *rootNode;
		int nodes;
		int totalNumberEntries;
		int totalNodeInBtrees;
		int page;
				int slot;
				int begin;
				int end;

}bTreeInfo;



#define MAXVALUEPERNODE 100


#define MAKE_VARSTRING(var)				\
do {							\
var = (StringRecordConverter *)calloc(1, sizeof(StringRecordConverter));	\
var->sizeofString = 0;					\
var->sizeOfBuffer = 100;					\
var->data = calloc(1, 100);				\
} while (0)

#define FREE_VARSTRING(var)			\
do {						\
free(var->data);				\
free(var);					\
} while (0)

#define GET_STRING(result, var)			\
do {						\
result = calloc(1, var->sizeofString + 1);		\
strncpy(result, var->data, var->sizeofString);	\
result[var->sizeofString] = '\0';			\
} while (0)

#define RETURN_STRING(var)			\
do {						\
char *resultStr;				\
GET_STRING(resultStr, var);			\
FREE_VARSTRING(var);			\
return resultStr;				\
} while (0)

#define ENSURE_SIZE(var,newsize)				\
do {								\
if (var->sizeOfBuffer < newsize)					\
{								\
int newbufsize = var->sizeOfBuffer;				\
while((newbufsize *= 2) < newsize);			\
var->data = realloc(var->data, newbufsize);			\
}								\
} while (0)

#define APPEND_STRING(var,string)					\
do {									\
ENSURE_SIZE(var, var->sizeofString + strlen(string));			\
strncpy(var->data + var->sizeofString, string, strlen(string));		\
var->sizeofString += strlen(string);					\
} while(0)

#define APPEND(var, ...)			\
do {						\
char *tmp = calloc(1, 10000);			\
sprintf(tmp, __VA_ARGS__);			\
APPEND_STRING(var,tmp);			\
free(tmp);					\
} while(0)


#endif /* ASSIGN4_ASSIGN4_BTREE_MGR_ASSN_H_ */
