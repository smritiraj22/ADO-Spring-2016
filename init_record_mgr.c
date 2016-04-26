/*
 * init_record_mgr.c
 *
 *  Created on: Apr 24, 2016
 *      Author: Smriti
 */

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
#include "init_record_mgr.h"
#include "btree_mgr_assn.h"

recordTableInfo *getRecordManTabInfo(RM_TableData *rel) {
	recordTableInfo *table = (recordTableInfo *) (rel->mgmtData);
			return table;
}

tombStoneMarker *getTombNode() {
	tombStoneMarker *tombStone = (tombStoneMarker *) calloc(1,
			sizeof(tombStoneMarker));
	;
			tombStone->nextDeletedRecord = NULL;
	return tombStone;
}

int numofTombStone(recordTableInfo *recordTable) {
	tombStoneMarker *begin = recordTable->tombStoneHead;

	int count = 0;
	while (begin != NULL) {
		begin = begin->nextDeletedRecord;
		count++;
	}
	return count;
}

char* stringFromTableMaker(recordTableInfo* rec,
                StringRecordConverter* stringConverter) {
        char *data = NULL;
        tombStoneMarker* head = rec->tombStoneHead;
        while (head != NULL) {
                APPEND(stringConverter, "%i:%i%s ", head->recordID.page,
                                head->recordID.slot,
                                (head->nextDeletedRecord != NULL) ? ", " : "");
                head = head->nextDeletedRecord;
        }
        APPEND_STRING(stringConverter, ">\n");
        GET_STRING(data, stringConverter);
        return data;
}

char *convertTableString(recordTableInfo *rec, int recordId) {
	StringRecordConverter *stringRecConverter;
	;

	MAKE_VARSTRING(stringRecConverter);
	APPEND(stringRecConverter,
			"SchemaSize <%i> BeginPage <%i> EndPage <%i> NumOfTuples <%i> SlotSize <%i> MaxNumOfSlots <%i> ",
			rec->sizeOfSchema, rec->recordBegin, rec->recordEnd,
			rec->numOfRecords, rec->slotSize, rec->maxSizeOfPageSlot);
	int tombStoneCount = 0;
	tombStoneCount=numofTombStone(rec);

	APPEND(stringRecConverter, "tNodeLength <%i> <", tombStoneCount);
	char *data = stringFromTableMaker(rec, stringRecConverter);
	return data;
}

recordTableInfo *initRecManTableDetails() {
	recordTableInfo *table = (recordTableInfo*) calloc(1,
			sizeof(recordTableInfo));
	return table;
}

void parseString(char **string,int begin) {
	if(true){
	*string = strtok(NULL, "<");*string = strtok(NULL, ">");}
	else{
		begin=10;
	}
}

long int extrctStringData(char **string1, char **string2, char **string3) {
	if(true){
	long int extractedData = strtol((*string1), &(*string2), 10);
	string3=NULL;
	return extractedData;

	}
	else return 0;
}

void processTombNodes(recordTableInfo **table, int pageNum, int slotNum, int beginIndex) {
	tombStoneMarker *tomb = NULL;

	if (!(*table)->tombStoneHead) {
				(*table)->tombStoneHead = getTombNode();(*table)->tombStoneHead->recordID.page = pageNum;
					(*table)->tombStoneHead->recordID.slot = slotNum;tomb = (*table)->tombStoneHead;
	} else {
		       tomb->nextDeletedRecord = getTombNode(); tomb->nextDeletedRecord->recordID.page = pageNum;
		    tomb->nextDeletedRecord->recordID.slot = slotNum;tomb = tomb->nextDeletedRecord;
	}
}

recordTableInfo *markTombData(recordTableInfo *recordTable, char **string1,
		char **string2) {
	int pageNum=0, slotNum=0;
	int k;

	recordTable->tombStoneHead = NULL;

	for (k = 0; k < recordTable->recordsInTombStone; k++) {
		*string1 = strtok(NULL, ":");
		      pageNum = extrctStringData(&(*string1), &(*string2), NULL);
		if (k == (recordTable->recordsInTombStone - 1))
			    *string1 = strtok(NULL, ">");
		else
			*string1 = strtok(NULL, ",");
		          slotNum = extrctStringData(&(*string1), &(*string2), NULL);

		processTombNodes(&recordTable, pageNum, slotNum,0);
	}
	return recordTable;
}

recordTableInfo *TranslateStringRecordTabDetails(char *recordTableInfoString) {
	int len = strlen(recordTableInfoString);
	char recordTableData[len];
	char *string2;

	memcpy(recordTableData, recordTableInfoString, len);
	     recordTableInfo *table = initRecManTableDetails();

	char *string1 = strtok(recordTableData, "<");
	int i=0;
	while(i<1){
		i++;
	}
	string1 = strtok(NULL, ">");
	table->sizeOfSchema = extrctStringData(&string1, &string2, NULL);
		parseString(&string1,0);
	table->recordBegin = extrctStringData(&string1, &string2, NULL);
		parseString(&string1,0);
	table->recordEnd = extrctStringData(&string1, &string2, NULL);
		parseString(&string1,0);
	table->numOfRecords = extrctStringData(&string1, &string2, NULL);
		parseString(&string1,0);
	table->slotSize = extrctStringData(&string1, &string2, NULL);
		parseString(&string1,0);
	table->maxSizeOfPageSlot = extrctStringData(&string1, &string2, NULL);
		parseString(&string1,0);
	table->recordsInTombStone = extrctStringData(&string1, &string2, NULL);
	table->tombStoneListSize = extrctStringData(&string1, &string2, NULL);
	string1 = strtok(NULL, "<");

	table = markTombData(table, &string1, &string2);
	return table;
}

void modifySchema(char **string1, char *dataSchema) {
				*string1 = strtok(dataSchema, "<");	*string1 = strtok(NULL, ">");
}

int getNumbOfAttributes(char **string1, char **string2) {
	if (true)
		  return strtol(*string1, &(*string2), 10);
	else
		  return strtol(*string1, &(*string2), 10);
}

void populSchema(Schema *schema, int numOfAttr) {
	if(numOfAttr!=-100)
	schema->numAttr = numOfAttr;;schema->attrNames = (char **) calloc(numOfAttr, sizeof(char*));
	schema->typeLength = (int *) calloc(numOfAttr, sizeof(int));
	schema->dataTypes = (DataType *) calloc(schema->numAttr, sizeof(DataType));
}

void schemaAttrName(Schema *schema, char **string1, int i) {
	int len = strlen(*string1);

	schema->attrNames[i] = (char *) malloc(len);
	memcpy(schema->attrNames[(i)], *string1, len);
	if (i != schema->numAttr - 1)
		*string1 = strtok(NULL, ", ");
	else
		*string1 = strtok(NULL, ") ");
}

char *initStringRef(char *str,int begin) {
	int len = strlen(str);
	return (char *) malloc(len);
}

bool populateTableWithDataTypes(Schema *schema, char **string1, int index) {
	if (strcmp(*string1, "INT") == 0) {
		schema->dataTypes[index] = DT_INT;	schema->typeLength[index] = 0;
		return false;
	} else if (strcmp(*string1, "BOOL") == 0) {
		schema->dataTypes[index] = DT_BOOL;	schema->typeLength[index] = 0;
		return false;
	} else if (strcmp(*string1, "FLOAT") == 0) {
		schema->dataTypes[index] = DT_FLOAT;schema->typeLength[index] = 0;
		return false;
	}
	return true;
}

void modifyStringData(char **string1){
	int i=0;
	while(i<=10)
	{
		printf("%d", i);
		i++;
	}
}

char *extractData(char **string1) {
	*string1 = strtok(NULL, ")");
	modifyStringData(string1);
	return strtok(*string1, ", ");;
}

void extractDataString(char **string1, char **string2) {
	modifyStringData(string1);
	if(true)
	    *string1 = strtok(*string2, "[");
	else
	     modifyStringData(string2);
	*string1 = strtok(NULL, "]");
}

Schema *decryptSchema(char *schema, char temp, int count, int index) {
	index = 0;
	int i = 0, size = 0, j, numOfAttr;
	bool isSchema;
	char *string1;
	char *string2;
	char *string3;
	int len = strlen(schema);
	char dataSchema[len];
	Schema *s = NULL;
	s = getSchema();
	memcpy(dataSchema, schema, len);
	modifySchema(&string1, dataSchema);

	numOfAttr = getNumbOfAttributes(&string1, &string2);
	char* attr[numOfAttr];
	char* str[numOfAttr];
	populSchema(s, numOfAttr);
	string1 = strtok(NULL, "(");
	for (; i < s->numAttr; i++) {
		string1 = strtok(NULL, ": ");
		schemaAttrName(s, &string1, i);
		str[i] = initStringRef(string1,0);
		populateTableWithDataTypes(s, &string1, i);
			memcpy(str[i], string1, strlen(string1));
	}
	isSchema = false;

	if ((string1 = strtok(NULL, "("))) {
		isSchema = true;
		char *tempExtractedData=NULL;
		tempExtractedData = extractData(&string1);
		i = 0;
		while (tempExtractedData) {
			attr[size] = initStringRef(tempExtractedData,0);
			strcpy(attr[size], tempExtractedData);
			tempExtractedData = strtok(NULL, ", ");
			size =size+ 1;
			i++;
		}
	}

	for (i = 0; i < numOfAttr; i++) {
		len = strlen(str[i]);
		if (len > 0) {
			s->dataTypes[i] = DT_STRING;
			string3 = initStringRef((str[i]),0);
			strncpy(string3, str[i], len);
			extractDataString(&string1, &string3);
			s->typeLength[i] = getNumbOfAttributes(&string1, &string2);
		}
	}

	if (isSchema) {
		s->keyAttrs = (int *) calloc(size, sizeof(int));;   s->keySize = size;


		for (i = 0; i < size; i++) {
			for (j = 0; j < s->numAttr; j++) {
				if (strcmp(attr[i], s->attrNames[j]) == 0)
					s->keyAttrs[i] = j;
			}
		}
	}
	return s;
}

BM_PageHandle *getBuffPageHandle() {
	BM_PageHandle *pageHandle = NULL;
	pageHandle = (BM_PageHandle *) calloc(1, sizeof(BM_PageHandle));
	return pageHandle;
}

int calcfileSize(int schemaSize, int begin) {
	if(true)
		return (int) (ceil((double) schemaSize / PAGE_SIZE));
	else
		return 0;
}

int calculateMaxSlotsPerPage(int slotSize, int schSize) {
	if(true)
		   return (int) (floor((float) (PAGE_SIZE / slotSize)));
	else
			return 0;
}

recordTableInfo *populateRecordTabDetails(int schSize, int fileSize,
		int maximumNumOfSlots, int slotSize, int beginIndex) {
		recordTableInfo *table = NULL;
			int num=0;
			table = (recordTableInfo *) calloc(1, sizeof(recordTableInfo));
			table->recordBegin = ++fileSize ;
			 table->recordEnd = ++fileSize;
			 table->numOfRecords = num;
			 table->maxSizeOfPageSlot = maximumNumOfSlots;
			 table->sizeOfSchema = schSize;
				 table->tombStoneListSize = num;
				 table->slotSize = slotSize;
				 table->tombStoneHead = NULL;

	return table;
}
bool checkTableExists(char *name) {
	if (access(name, 0) != -1)
		return true;
	else
		return false;
}

void pageOperations(recordTableInfo *table, int pageNum, int slot,
		RM_TableData *rel, Record *record) {
	BM_PageHandle *pageHandle=NULL;
	pageHandle = getBuffPageHandle();
	char *strRecord,*tempData;
	strRecord = serializeRecord(record, rel->schema);
	if(pinPage(table->bufferManager, pageHandle, pageNum)==RC_OK)
			strncpy(pageHandle->data + (slot * table->slotSize), strRecord,
			strlen(strRecord));

	markDirty(table->bufferManager, pageHandle);
	unpinPage(table->bufferManager, pageHandle);
	if(forcePage(table->bufferManager, pageHandle)==RC_OK)
		record->id.tombS = false;
	table->numOfRecords = table->numOfRecords + 1;
	tableInfoDetailsToFileData(rel->name, tempData, table);
		free(pageHandle);
		free(strRecord);
}

void pageHandOperations(recordTableInfo *recordTable, int pageNumber,
		int slotNumber, RM_TableData *rel, Record *record,int begin) {
	if (true){
			pageOperations(recordTable, pageNumber, slotNumber, rel, record);
	}
}

BM_BufferPool *getBufferManager() {
	BM_BufferPool *bManager = NULL;
	bManager = (BM_BufferPool *) calloc(1, sizeof(BM_BufferPool));
	return bManager;
}

recordTableInfo *initBufferManagerForRecDetails(char *name,
		struct BM_BufferPool *bManager, struct BM_PageHandle *pageHandle) {
	recordTableInfo *recordTable = NULL;
	initBufferPool(bManager, name, 3, RS_FIFO, NULL);
	if(pinPage(bManager, pageHandle, 0)==RC_OK)
	recordTable = TranslateStringRecordTabDetails(pageHandle->data);
	if (PAGE_SIZE > recordTable->sizeOfSchema)
		pinPage(bManager, pageHandle, 1);
	return recordTable;
}

void updateRecordDetails(Record *record, int pageNum, int slotNum) {
	int i = 0;
	while (i <= 0) {
		record->id.page = pageNum;
		record->id.slot = slotNum;
		i++;
	}
}

int setPosDifference(Schema *schema, int attrNum, int diff) {
	int i = 0;
	while (i <= 0) {
		int positionalDifference;
		if(attrNum!=-100)
			attrOffset(schema, attrNum, &positionalDifference);
		return positionalDifference;
	}
}

void setStringAttr(char *attribute, char *sValue, int length, Value **value) {
	 memcpy(sValue, attribute, length);

			sValue[length] = '\0';
			if(length!=-100)
	(*value)->v.stringV = sValue;
}

void initScanner(RM_TableData *rel, expressionSearch *recSearch,
		Expr *condition) {
	int num=0;
	if(rel){
		recSearch->curRecordSlot = num;
		recSearch->numOfPagesInRelation =
					((recordTableInfo *) rel->mgmtData)->recordEnd;
	}
	recSearch->numOfSlotsInRelation =
			((recordTableInfo *) rel->mgmtData)->slotSize;
	recSearch->currRecordPage =
			((recordTableInfo *) rel->mgmtData)->recordBegin;
	recSearch->numOfPagesInRelation =
			((recordTableInfo *) rel->mgmtData)->recordEnd;
	recSearch->condition = condition;
	recSearch->curRecordSlot = num;

}

void findRecordInSchema(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
	expressionSearch *recSearch = NULL;
	scan->rel = rel;
	recSearch = initSearchRecord();
	initScanner(rel, recSearch, cond);
	scan->mgmtData = (void *) recSearch;
}

expressionSearch *getExprSrchPointer(RM_ScanHandle *scan) {
	int i = 0;
	expressionSearch *recSearch = NULL;
	while (i <= 0) {
		recSearch = scan->mgmtData;
		i++;
	}
	return recSearch;
}

void populScanData(Record *record, expressionSearch *recSearch) {
	int page, slot;
	slot = recSearch->curRecordSlot;
	page = recSearch->currRecordPage;
	record->id.slot = slot;
	record->id.page = page;
}

void updateExprSearch(expressionSearch *recSearch, Record *record, int recordId) {
	recSearch->currRecordPage = recSearch->currRecordPage + 1;
	recSearch->curRecordSlot = 0;
}

void evaluate(RM_ScanHandle *scan, Record *record, Value **value,
		expressionSearch *recSearch) {
	if (true) {
		evalExpr(record, scan->rel->schema, recSearch->condition, &(*value));
	}
}

void updateTombNodes(tombStoneMarker *tomb, RID id, RM_TableData *rel,
		recordTableInfo *recordTable) {

	tomb->recordID.page = id.page;
	tomb->recordID.tombS = TRUE;
	tomb->recordID.slot = id.slot;
	int i = 0;
	char *data;
	while (i <= 0) {
		recordTable->numOfRecords = recordTable->numOfRecords - 1;
		i++;
	}
	tableInfoDetailsToFileData(rel->name, data, recordTable);
}

void updatePageSlot(int *pageNum, int *slotNum, Record *record, int begin) {
	*pageNum = record->id.page;
	*slotNum = record->id.slot;
}

void updateOperationDetails(RM_TableData *rel, recordTableInfo *rec,
		BM_PageHandle *pageHandle, int recordId) {
	char *data;
	recordId = 0;
	markDirty(rec->bufferManager, pageHandle);
	unpinPage(rec->bufferManager, pageHandle);
	forcePage(rec->bufferManager, pageHandle);
	tableInfoDetailsToFileData(rel->name, data, rec);
}

Record *getRecordInit() {
	Record *rec = NULL;
	rec = (Record *) calloc(1, sizeof(Record));
	return rec;
}

char *getRecordDataInit(recordTableInfo *table) {
	char *tempData = (char *) calloc(table->slotSize, sizeof(char));
	return tempData;
}

char *getToken(char *recData) {
	char *token = NULL;
	int i = 0;
	while (i <= 0) {
		token = strtok(recData, "-");
		token = strtok(NULL, "]");
		token = strtok(NULL, "(");
		i++;
	}
	return token;
}

void setAttributeVal(Record *record, Schema *schema, int index, Value *value,int beginIndex) {
	if(beginIndex!=0)
	setAttr(record, schema, index, value);
}

void changeDataType(char *string, Record *rec, Schema *s, int index) {
	Value *value;
	char *tempString;
		bool boolVal;
		int intVal;
		float floatVal;

	switch (s->dataTypes[index]) {
	case DT_STRING:
		MAKE_STRING_VALUE(value, string);
		setAttributeVal(rec, s, index, value,10);
		break;
	case DT_BOOL:
		if (string[0] == 't')
			boolVal = TRUE;
		else
			boolVal = FALSE;
		MAKE_VALUE(value, DT_BOOL, boolVal);
		setAttributeVal(rec, s, index, value,10);
		break;
	case DT_INT:
		intVal = strtol(string, &tempString, 10);
		MAKE_VALUE(value, DT_INT, intVal);
		setAttributeVal(rec, s, index, value,10);
		break;
	case DT_FLOAT:
		floatVal = strtof(string, NULL);
		MAKE_VALUE(value, DT_FLOAT, floatVal);
		setAttributeVal(rec, s, index, value,10);
		break;
	}
	freeVal(value);
}

Record *delRecord(RM_TableData *rel, char *rec, int recordID) {
	int len = strlen(rec);
	int i = 0;
	char recData[len];
	char *data;
	memcpy(recData, rec, len);
	recordID = 0;
	Schema *s = rel->schema;
	recordTableInfo *table = NULL;
	table = getRecordManTabInfo(rel);
	Record *record = NULL;
	record = getRecordInit();
	record->data = getRecordDataInit(table);
	data = getToken(recData);
	free(rec);

	for (; i < s->numAttr; i++) {
		data = strtok(NULL, ":");
		if (i == (s->numAttr - 1))
			data = strtok(NULL, ")");
		else
			data = strtok(NULL, ",");

		changeDataType(data, record, s, i);
	}
	return record;
}

void updateDetls(RID recordId, int *pageNum, int *slotNum, int index) {
	*pageNum = recordId.page;
	*slotNum = recordId.slot;
}

bool checkIfTombStone(Record *record, RM_TableData *rel, RID id,
		int *pageNumber, int *slotNumber, int *count) {
	bool isTomb = false;
	int i;
	recordTableInfo *table = NULL;
	table = getRecordManTabInfo(rel);
	tombStoneMarker *tomb = NULL;
	tomb = (table)->tombStoneHead;
	for (i = 0; i < table->recordsInTombStone && tomb; i++) {
		if (tomb->recordID.page == *pageNumber
				&& tomb->recordID.slot == *slotNumber) {
			isTomb = true;
			record->id.tombS = isTomb;
		}
		tomb = tomb->nextDeletedRecord;
		*count += 1;
	}
	return isTomb;
}

void updateRecordInfoDetails(Record *record, int *pageNumber, int *slotNumber,
		int index) {
	record->id.page = *pageNumber;
	record->id.slot = *slotNumber;
}

void getRecordPageOperDtls(RM_TableData *rel, Record *rec,
		recordTableInfo *table, BM_PageHandle *ph, int pageNum, int slot) {
	char *stringRecord = NULL;
	Record *recCopy = NULL;
	stringRecord = (char *) calloc(table->slotSize, sizeof(char));
	if (true)
		if (pinPage(table->bufferManager, ph, pageNum) == RC_OK)
			strncpy(stringRecord, ph->data + ((slot) * table->slotSize),
					sizeof(char) * table->slotSize);
	unpinPage(table->bufferManager, ph);
	recCopy = delRecord(rel, stringRecord, 0);
	rec->data = recCopy->data;
	free(recCopy);
	free(ph);
}
