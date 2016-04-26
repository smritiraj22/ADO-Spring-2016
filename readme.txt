*********************************************************************
Process to Run the B+ tree program:-

1)Extract files
2)In unix terminal navigate to extracted directory
3)Run "make" command on unix terminal as it contains all the file
4)Run "./test_assign4_1" 
5)Result is printed


extern RC initIndexManager (void *mgmtData); 
initialtes Index Manager tree
extern RC shutdownIndexManager ();
shutdown Index Manager tree

extern RC createBtree (char *idxId, DataType keyType, int n);
creates B tree index
extern RC openBtree (BTreeHandle **tree, char *idxId);
opens and loads B tree index
extern RC closeBtree (BTreeHandle *tree);
closes B tree index
extern RC deleteBtree (char *idxId);
deletes B tree index


extern RC getNumNodes (BTreeHandle *tree, int *result);
gets number of nodes in the index
extern RC getNumEntries (BTreeHandle *tree, int *result);
gets number of enties in the index
extern RC getKeyType (BTreeHandle *tree, DataType *result);
gets the type of key in the index


extern RC findKey (BTreeHandle *tree, Value *key, RID *result);
finds key in index
extern RC insertKey (BTreeHandle *tree, Value *key, RID rid);
inserts key in index
extern RC deleteKey (BTreeHandle *tree, Value *key);
deletes key in index
extern RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle);
opens the tree scan
extern RC nextEntry (BT_ScanHandle *handle, RID *result);
gets the next entry in the index
extern RC closeTreeScan (BT_ScanHandle *handle);
closes the tree scan

extern char *printTree (BTreeHandle *tree);
prints the index tree structure
