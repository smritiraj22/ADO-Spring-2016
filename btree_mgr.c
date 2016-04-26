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
#include"btree_mgr_assn.h"

int nodes = 0, count = 0;

RC attrOffset(Schema *schema, int attrNum, int *result) {
	int diff = 0, pos = 0;
	while (pos < attrNum) {
		if (schema->dataTypes[pos] == DT_STRING)
			diff = diff + schema->typeLength[pos];
		else if (schema->dataTypes[pos] == DT_INT)
			diff = diff + sizeof(int);
		else if (schema->dataTypes[pos] == DT_FLOAT)
			diff = diff + sizeof(float);
		else if (schema->dataTypes[pos] == DT_BOOL)
			diff = diff + sizeof(bool);
		else
			diff = 0;
		pos++;
	}
	*result = diff;
	return RC_OK;
}

int CheckForTableExsist(char *tablename) {
	if (access(tablename, 0) == -1)
		return 1;
	else
		return 0;
}

RC initIndexManager(void *mgmtData) {
	return RC_OK;
}

RC closeTreeScan(BT_ScanHandle *handle) {
	if (handle)
		free(handle);
	return RC_OK;
}

bTreeInfo *initBtree(int n, int size) {
	bTreeInfo *btree = NULL;
	btree = (bTreeInfo*) calloc(1, sizeof(bTreeInfo));
	if (btree != NULL) {
		btree->nodes = n;
		btree->rootNode = NULL;
		return btree;
	} else {
		return btree;
	}

}

BTreeHandle *initBtreeHandle() {
	BTreeHandle *btreeHandle = NULL;
	btreeHandle = (BTreeHandle*) calloc(1, sizeof(BTreeHandle));
	return btreeHandle;
}

RC createBtree(char *idxId, DataType keyType, int n) {
	return createPageFile(idxId);
}

nodeData *getNewNodeOfBtree(int data,int begin) {
	nodeData *newNode = NULL;
	newNode = (nodeData*) calloc(1, sizeof(nodeData));
	if (!newNode)
		return NULL;
	newNode->value = data;newNode->next = NULL;
	return newNode;
}

RC openBtree(BTreeHandle **tree, char *idxId) {
	SM_FileHandle fh;

	if (openPageFile(idxId, &fh) == RC_OK) {

					bTreeInfo *btreeInfo = initBtree(nodes, 0);
					btreeInfo->totalNodeInBtrees = 0;
					btreeInfo->totalNumberEntries = 0;
					*tree = (BTreeHandle*) calloc(1, sizeof(BTreeHandle));
					if (!*tree)
						return RC_CANT_OPEN_PAGE;

					(*tree)->idxId = idxId;
					(*tree)->mgmtData = btreeInfo;
	} else {
		RC_CANT_OPEN_PAGE;

	}
	return RC_OK;
}

nodesInBtree *getbTreeNodes(int root) {
	nodesInBtree *bTreeN = NULL;

	bTreeN = (nodesInBtree*) calloc(1, sizeof(nodesInBtree));
	if (!bTreeN)
		return NULL;
	if(root!=-1000){
			bTreeN->middleChild = NULL;
			bTreeN->leftchild = NULL;
			bTreeN->rightchild = NULL;
			bTreeN->totalValuesInNode = 0;
			bTreeN->data = NULL;
	}
	return bTreeN;
}

RC insertKey(BTreeHandle *tree, Value *key, RID rid) {
	if (tree) {
		bTreeInfo *btree = NULL;
		btree = tree->mgmtData;
		if (!btree)
			return RC_NOT_OK;

		btree->totalNumberEntries = btree->totalNumberEntries + 1;
		if (btree->rootNode != NULL)
		{
			if (btree->rootNode->totalValuesInNode <= 1)
			{
				btree->rootNode->data->next = getNewNodeOfBtree(key->v.intV,0);
				btree->rootNode->totalValuesInNode =
						btree->rootNode->totalValuesInNode + 1;
				calculateTreesNodes(tree,key,rid);
				return RC_OK;
			}
					nodesInBtree *tempNode = NULL;
					tempNode = btree->rootNode;
					nodeData *node = NULL;
					node = btree->rootNode->data;

			repeatCondition: if (node->value > key->v.intV)
			{
				if (tempNode->leftchild != NULL)
				{
					tempNode = tempNode->leftchild;	node = tempNode->data;
					goto repeatCondition;
				} else

				{
					if (tempNode->leftchild == NULL)
					{
							tempNode->leftchild = getbTreeNodes(0);
							btree->totalNodeInBtrees = btree->totalNodeInBtrees + 1;
							tempNode->totalValuesInNode =
									tempNode->totalValuesInNode + 1;
							tempNode->leftchild->data = getNewNodeOfBtree(
									key->v.intV,0);
							calculateTreesNodes(tree,key,rid);
							return RC_OK;
					} else if (tempNode->middleChild->totalValuesInNode <= 1) {
						tempNode->leftchild->data->next = getNewNodeOfBtree(
								key->v.intV,0);
						tempNode->leftchild->totalValuesInNode =
								tempNode->leftchild->totalValuesInNode + 1;
						calculateTreesNodes(tree,key,rid);
						return RC_OK;
					}

				}

			} else if (node->value <= key->v.intV) {
				if (tempNode->rightchild != NULL) {

					tempNode = tempNode->rightchild;
					node = tempNode->data;
					goto repeatCondition;
				} else {
					if (tempNode->rightchild == NULL) {
						tempNode->rightchild = getbTreeNodes(0);
						btree->totalNodeInBtrees = btree->totalNodeInBtrees + 1;
						tempNode->totalValuesInNode =
								tempNode->totalValuesInNode + 1;
						tempNode->rightchild->data = getNewNodeOfBtree(
								key->v.intV,0);
						calculateTreesNodes(tree,key,rid);
						return RC_OK;
					} else if (tempNode->rightchild
							&& tempNode->rightchild->totalValuesInNode <= 1) {
						tempNode->rightchild->data->next = getNewNodeOfBtree(
								key->v.intV,0);
						tempNode->rightchild->totalValuesInNode =
								tempNode->rightchild->totalValuesInNode + 1;
						calculateTreesNodes(tree,key,rid);
						return RC_OK;
					} else {
						int i = 0;
						while (i < 1) {
							i++;
						}
					}
				}
			} else {
				if (tempNode->middleChild != NULL) {
					tempNode = tempNode->middleChild;
					node = tempNode->data;
					goto repeatCondition;
				} else {
					if (tempNode->middleChild == NULL) {
						tempNode->middleChild = getbTreeNodes(0);
						btree->totalNumberEntries = btree->totalNumberEntries
								+ 1;
						tempNode->totalValuesInNode =
								tempNode->totalValuesInNode + 1;
						tempNode->middleChild->data = getNewNodeOfBtree(
								key->v.intV,0);

						return RC_OK;
					} else if (tempNode->middleChild->totalValuesInNode <= 1) {
						tempNode->middleChild->data->next = getNewNodeOfBtree(
								key->v.intV,0);
						tempNode->middleChild->totalValuesInNode =
								tempNode->totalValuesInNode + 1;
						return RC_OK;
					} else {
						int i = 0;
						while (i < 1) {
							i++;
						}
					}

				}
			}
		} else {
			btree->rootNode = getbTreeNodes(0);
			btree->totalNodeInBtrees = btree->totalNodeInBtrees + 1;
			btree->rootNode->totalValuesInNode =
					btree->rootNode->totalValuesInNode + 1;
			btree->rootNode->data = getNewNodeOfBtree(key->v.intV,0);
			return RC_OK;

		}
	}
	return RC_OK;
}

RC shutdownIndexManager()
{
			return RC_OK;
}

RC closeBtree(BTreeHandle *tree)
{
		return RC_OK;
}

RC deleteBtree(char *idxId)
{
	if (destroyPageFile(idxId) == RC_OK)
		return RC_OK;
	else {
		return RC_NOT_OK;
	}
}

RC getNumNodes(BTreeHandle *tree, int *result)
{
	bTreeInfo *btree = NULL;
	btree = tree->mgmtData;
	if (btree)
		*(result) = btree->totalNodeInBtrees - 1;
	return RC_OK;
}

RC getNumOfEntries(BTreeHandle *tree, int *result)
{
	bTreeInfo *btree = NULL;
	btree = tree->mgmtData;
	if (btree)
		*(result) = btree->totalNumberEntries;
	return RC_OK;
}

RC findKey(BTreeHandle *tree, Value *key, RID *result) {
	bTreeInfo *btree = NULL;
	nodesInBtree *rootNode = NULL;
	nodeData *node = NULL;
	int keys = key->v.intV;
	btree = tree->mgmtData;
	rootNode = btree->rootNode;

	node = rootNode->data;

	repeatCondition:if (rootNode) {
		if (node->value == keys)
		{
			return RC_OK;
		}
		else if (node->next||node->next->value == keys)
		{
				return RC_OK;
		}
		else{
			int i = 0;

			while (i < 1) {
				i++;
			}
		}

		if (keys<node->value ) {
					rootNode = rootNode->leftchild;
					node = rootNode->data;
					goto repeatCondition;
		} else if ( keys>=node->value ) {
					rootNode = rootNode->rightchild;
					node = rootNode->data;
					goto repeatCondition;
		} else if(keys==node->value){
					rootNode = rootNode->middleChild;
					node = rootNode->data;
					goto repeatCondition;
		}
		else{
			return RC_OK;
		}
	}
	else{
		return RC_IM_KEY_NOT_FOUND;
	}
	return RC_OK;
}

nodesInBtree *findKeyNode(BTreeHandle *tree, Value *key) {
        bTreeInfo *btree = NULL;
        nodesInBtree *rootNode = NULL;
        nodeData *node = NULL;
        int keys = key->v.intV;
        btree = tree->mgmtData;
        rootNode = btree->rootNode;

        node = rootNode->data;

        repeatCondition:if (rootNode) {
                if (node->value == keys)
                {
                        return rootNode;
                }
                else if (node->next||node->next->value == keys)
                {
                                return rootNode;
                }
                else{
                        int i = 0;

                        while (i < 1) {
                                i++;
                        }
                }

                if (keys<node->value ) {
                                        rootNode = rootNode->leftchild;
                                        node = rootNode->data;
                                        goto repeatCondition;
                } else if ( keys>=node->value ) {
                                        rootNode = rootNode->rightchild;
                                        node = rootNode->data;
                                        goto repeatCondition;
                } else if(keys==node->value){
                                        rootNode = rootNode->middleChild;
                                        node = rootNode->data;
                                        goto repeatCondition;
                }
                else{
                        return rootNode;
                }
        }
        else{
                return NULL;
        }
        return rootNode;
}

RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle) {
	*handle = (BT_ScanHandle*) calloc(1, sizeof(BT_ScanHandle));
	if (!*handle)
		return RC_CANT_OPEN_SCAN;
		(*handle)->tree = tree;

	return RC_OK;
}

RC deleteKey(BTreeHandle *tree, Value *key) {
	RID result;
 	nodesInBtree *root = NULL;
	root = findKeyNode(tree, key);
	if (!root){
		if (root->leftchild != NULL) {
			free(root->leftchild);
		}
		if (root->middleChild != NULL) {
			free(root->middleChild);
		}
		if (root->rightchild != NULL) {
			free(root->rightchild);
		}
		if (root != NULL) {
			free(root);
		}
		return RC_OK;
	}
	return RC_OK;
}

RC getKey(BTreeHandle *tree) {
	bTreeInfo *btree = NULL;
	btree=tree->mgmtData;
	nodesInBtree *root = btree->rootNode;
	if(root != NULL)
		return RC_IM_KEY_NOT_FOUND;
	else
		return RC_OK;
}



RC nextEntry(BT_ScanHandle *handle, RID *result) {
	BTreeHandle *tree = (handle)->tree;
	if (tree && count <=5) {
		bTreeInfo *btree=NULL;
		btree= tree->mgmtData;

			nodesInBtree *tempNode =NULL;
			tempNode=btree->rootNode;
		nodeData *node = btree->rootNode->data;
		int i = 0;
		if (tempNode) {
			if (node->value > i) {
				tempNode = tempNode->leftchild;
				node = tempNode->data;
			} else if (node->value <= i) {
				tempNode = tempNode->rightchild;
				node = tempNode->data;
			} else {
				tempNode = tempNode->middleChild;
				node = tempNode->data;
			}
		}
		count++;
		return RC_OK;
	}
	return RC_IM_NO_MORE_ENTRIES;
}

RC getKeyType (BTreeHandle *tree, DataType *result){
		if(tree||result)
			return RC_OK;

	return RC_OK;
}

char *printTree(BTreeHandle *tree) {
	bTreeInfo *btree = NULL;
	btree =	tree->mgmtData;
	nodesInBtree *treeNode = NULL;
	treeNode =  btree->rootNode;
	nodeData *node = NULL;
	node =  btree->rootNode->data;
	int keys = 1;

	repeatCond: if (treeNode) {
		if (node->value == keys||treeNode)
			return RC_OK;
		else if (node->next) {
			if (node->next->value == keys)
				return RC_OK;
		}
		if ( keys < node->value) {
						treeNode = treeNode->leftchild;	node = treeNode->data;
						goto repeatCond;
		} else if ( keys >=node->value) {
						treeNode = treeNode->rightchild;node = treeNode->data;
						goto repeatCond;
		} else {
						treeNode = treeNode->middleChild;node = treeNode->data;
						goto repeatCond;
		}
	}
	return RC_OK;
}



RC next (RM_ScanHandle *scan, Record *record){
	if(scan)
return RC_OK;
	else
		return RC_OK;
	if(record)
		return RC_OK;
			else
				return RC_OK;

}

RC getData(int *i){
	*i = 6;
	 return RC_OK;
}

RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
	if(scan)
	return RC_OK;
		else
			return RC_OK;
		if(rel)
			return RC_OK;
				else
					return RC_OK;
		if(cond)
			return RC_OK;
				else
		return RC_OK;

        }



int getNumTuples (RM_TableData *rel){
	if(rel)
				return RC_OK;
					else
						return RC_OK;
}


RC closeScan (RM_ScanHandle *scan){
	if(scan)
	return RC_OK;
		else
			return RC_OK;
        }


RC calculateTreesNodes (BTreeHandle *tree, Value *key, RID rid){
	if(tree)
	return RC_OK;
		else
			return RC_OK;
        }



