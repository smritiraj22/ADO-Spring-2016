/*
 * init_record_mgr.h
 *
 *  Created on: Apr 24, 2016
 *      Author: Smriti
 */

#ifndef ASSIGN4_ASSIGN4_INIT_RECORD_MGR_H_
#define ASSIGN4_ASSIGN4_INIT_RECORD_MGR_H_
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<math.h>
#include<unistd.h>
#include"storage_mgr.h"
#include"buffer_mgr.h"
#include"record_mgr.h"
#include"tables.h"
#include"definition.h"
#include "init_buffer_manager.h"
#include "record_mgr_assign.h"


extern recordTableInfo *getRecordManTabInfo(RM_TableData *rel);
extern tombStoneMarker *getTombNode();
extern int numofTombStone(recordTableInfo *recordTable);
extern char* stringFromTableMaker(recordTableInfo* rec,
		StringRecordConverter* stringConverter);

extern char *convertTableString(recordTableInfo *rec,int recordId);
extern recordTableInfo *initRecManTableDetails();
extern void parseString(char **string,int begin);

extern long int extrctStringData(char **string1, char **string2,char **string3);

extern void processTombNodes(recordTableInfo **table, int pageNum, int slotNum, int beginIndex);

extern recordTableInfo *markTombData(recordTableInfo *recordTable, char **string1,
 		char **string2);

extern recordTableInfo *TranslateStringRecordTabDetails(char *recordTableInfoString);

extern void modifySchema(char **string1, char *dataSchema);

extern int getNumbOfAttributes(char **string1, char **string2);
extern void populSchema(Schema *schema, int numOfAttr);
extern void schemaAttrName(Schema *schema, char **string1, int i);

extern char *initStringRef(char *str,int begin);

extern bool populateTableWithDataTypes(Schema *schema, char **string1, int index);
extern char *extractData(char **string1);
extern void extractDataString(char **string1, char **string2);
extern Schema *decryptSchema(char *schema, char temp, int count, int index);
extern BM_PageHandle *getBuffPageHandle();
extern int calcfileSize(int schemaSize, int b);
extern int calculateMaxSlotsPerPage(int slotSize, int schSize);
extern recordTableInfo *populateRecordTabDetails(int schSize, int fileSize,
		int maximumNumOfSlots, int slotSize, int beginIndex);
extern bool checkTableExists(char *name);
extern void pageOperations(recordTableInfo *table, int pageNum, int slot,
 		RM_TableData *rel, Record *record);
extern void pageHandOperations(recordTableInfo *recordTable, int pageNumber,
		int slotNumber, RM_TableData *rel, Record *record,int begin);
extern BM_BufferPool *getBufferManager();

extern recordTableInfo *initBufferManagerForRecDetails(char *name,
		struct BM_BufferPool *bManager, struct BM_PageHandle *pageHandle);
extern void updateRecordDetails(Record *record, int pageNum, int slotNum);

extern int setPosDifference(Schema *schema, int attrNum, int c);

extern void setStringAttr(char *attribute, char *sValue, int length,
 		Value **value);
extern void initScanner(RM_TableData *rel, expressionSearch *recSearch,
 		Expr *condition);
extern void findRecordInSchema(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond);
extern void getRecordPageOperDtls(RM_TableData *rel, Record *rec,
 		recordTableInfo *table, BM_PageHandle *ph, int pageNum, int slot);
extern void updateRecordInfoDetails(Record *record, int *pageNumber, int *slotNumber,int index);
extern bool checkIfTombStone(Record *record, RM_TableData *rel, RID id, int *pageNumber,
		int *slotNumber, int *count);
extern void updateDetls(RID recordId, int *pageNum, int *slotNum,int index);
extern Record *delRecord(RM_TableData *rel, char *rec,int recordID);
extern void changeDataType(char *string, Record *rec, Schema *s, int index);
extern void setAttributeVal(Record *record, Schema *schema, int loop, Value *value, int beginIndex);
extern char *getToken(char *recData);

extern char *getRecordDataInit(recordTableInfo *table);
extern Record *getRecordInit();
extern void updateOperationDetails(RM_TableData *rel, recordTableInfo *rec,
 		BM_PageHandle *pageHandle, int b);
extern void updatePageSlot(int *pageNum, int *slotNum, Record *record, int b);

extern void updateTombNodes(tombStoneMarker *tomb, RID id, RM_TableData *rel,
 		recordTableInfo *recordTable);
extern void evaluate(RM_ScanHandle *scan, Record *record, Value **value,
 		expressionSearch *recSearch);
extern void updateExprSearch(expressionSearch *recSearch,Record *record,int recordId);
extern void populScanData(Record *record, expressionSearch *recSearch);
extern expressionSearch *getExprSrchPointer(RM_ScanHandle *scan);


#endif /* ASSIGN4_ASSIGN4_INIT_RECORD_MGR_H_ */
