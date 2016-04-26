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

Schema *getSchema() {
	Schema *schema = (Schema *) calloc(1, sizeof(Schema));
	return schema;
}

int getNumTuples(RM_TableData *rel) {
	int numberOfTuples = ((recordTableInfo *) rel->mgmtData)->numOfRecords;
	return numberOfTuples;
}

RC openingFile(char *name, SM_FileHandle fHandle) {
	if (openPageFile(name, &fHandle) == RC_OK)
		return RC_OK;
	else
		return RC_CANT_OPEN_PAGE;
}

RC closeFile(SM_FileHandle fHandle) {
	return closePageFile(&fHandle);
}

RC writeToFile(SM_FileHandle fHandle, recordTableInfo *recordTable) {

	char *recordTableString = convertTableString(recordTable, 0);
	if (writeBlock(0, &fHandle, recordTableString) == RC_OK)
		free(recordTableString);
	return RC_OK;
}



RC writeSchemaToPhysicalTable(recordTableInfo *recordTable, SM_FileHandle fHandle,
		Schema *schema) {
	char *tableDetails=NULL;
	tableDetails = convertTableString(recordTable, 0);
	int index;
	char *schemaToString = NULL;
	if (writeBlock(0, &fHandle, tableDetails) == RC_OK) {
			schemaToString = serializeSchema(schema);
		if (writeBlock(1, &fHandle, schemaToString) == RC_OK){
			closePageFile(&fHandle);
		}}
	return RC_OK;
}

RC createTable(char *name, Schema *schema) {
	SM_FileHandle fh;
	if (createPageFile(name) == RC_OK) {
		int index = 0;
		int schSize = 0;
		int slotSize = 15;
		int dt = 0;
		int fileSize = 0;
		int maximumNumOfSlots = 0;

		for (; index < schema->numAttr; index++)
			schSize += strlen(schema->attrNames[index]);

		schSize += sizeof(int) * 2 + 2 * sizeof(int) * (schema->numAttr)
				+ sizeof(int) * (schema->keySize);

		fileSize = calcfileSize(schSize, 0);
		index = 0;
		while (index < schema->numAttr) {
			switch (schema->dataTypes[index]) {
			case DT_STRING:
				dt = schema->typeLength[index];
				break;
			case DT_BOOL:
				break;
			case DT_INT:
				dt = 5;
				break;
			case DT_FLOAT:
				dt = 10;
				break;
			}
			slotSize = slotSize + (dt + strlen(schema->attrNames[index]) + 2);
			index++;
		}
		maximumNumOfSlots = calculateMaxSlotsPerPage(slotSize, schSize);
		if (openPageFile(name, &fh) == RC_OK) {
			ensureCapacity((fileSize + 1), &fh);
			recordTableInfo *table_info = populateRecordTabDetails(schSize,
					fileSize, maximumNumOfSlots, slotSize, 0);
			writeSchemaToPhysicalTable(table_info, fh, schema);
		}
	}
	return RC_OK;
}

RC openTable(RM_TableData *rel, char *name) {
	if (access(name, 0) != 0) {
		return RC_TABLE_DOES_NOT_EXIST;
	} else {
		int records = 0;
		BM_BufferPool *buffMan = NULL;
		buffMan = getBufferManager();
		char tempData;
		BM_PageHandle *ph = NULL;
		ph = getBuffPageHandle();
		recordTableInfo *table = NULL;
		table = initBufferManagerForRecDetails(name, buffMan, ph);
		records = table->numOfRecords;
		rel->name = name;
		rel->schema = decryptSchema(ph->data, tempData, records, 0);
		table->bufferManager = buffMan;
		rel->mgmtData = table;
		free(ph);
		return RC_OK;

	}
}

RC closeTable(RM_TableData *rel) {
	shutdownBufferPool(((recordTableInfo *) rel->mgmtData)->bufferManager);
	freeMemoryOfRecordManager(rel);
	return RC_OK;
}

RC freeMemoryOfRecordManager(RM_TableData *rel) {
	free(rel->schema->keyAttrs);
	free(rel->schema->dataTypes);
	free(rel->schema->typeLength);
	free(rel->schema->attrNames);
	free(rel->mgmtData);
	free(rel->schema);
	return RC_OK;
}

RC deleteTable(char *name) {
	if (access(name, 0) == 0) {
		if (remove(name) == 0)
			return RC_OK;
		else
			RC_TABLE_DOES_NOT_EXIST;
	} else {
		RC_TABLE_DOES_NOT_EXIST;
	}
	return RC_OK;
}

RC initRecordManager (void *mgmtData)
		{
			return RC_OK;
		}

int getRecordSize(Schema *schema) {
	int recSize = 0;
	int i = 0;
	for (; i < schema->numAttr; i++) {
		switch (schema->dataTypes[i]) {

		case DT_STRING:
			recSize = recSize + schema->typeLength[i];
			break;

		case DT_BOOL:
			recSize = recSize + sizeof(bool);
			break;

		case DT_INT:
			recSize = recSize + sizeof(int);
			break;

		case DT_FLOAT:
			recSize = recSize + sizeof(float);
			break;
		}
	}
	return recSize;
}

Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes,
		int *typeLength, int keySize, int *keys) {
	Schema *schema = NULL;
	schema = (Schema *) calloc(1, sizeof(Schema));
	schema->keyAttrs = keys;
	schema->numAttr = numAttr;
	 schema->attrNames = attrNames;
	schema->keySize = keySize;
   	 schema->dataTypes = dataTypes;
	schema->typeLength = typeLength;

	return schema;
}

RC freeSchema(Schema *schema) {
	free(schema);
	return RC_OK;
}

int calSlotNumber(int pageNumber, recordTableInfo *recordTable) {
	int slotNumber = 0;
	if (true)
		slotNumber = recordTable->numOfRecords
				- ((pageNumber - recordTable->recordBegin)
						* recordTable->maxSizeOfPageSlot);
	return slotNumber;
}

int setPageNum(recordTableInfo *recordTable) {
	int pageNumber = recordTable->tombStoneHead->recordID.page;
	if (true)
		recordTable->tombStoneHead =
				recordTable->tombStoneHead->nextDeletedRecord;
	return pageNumber;
}

RC insertRecord(RM_TableData *rel, Record *record) {
	int pageNum = 0;
	int slotNum = 0, count = 0;
	recordTableInfo *table = getRecordManTabInfo(rel);
	if (table->tombStoneHead == NULL && true) {
		int pageNum = table->recordEnd;
		slotNum = calSlotNumber(pageNum, table);
		if (table->maxSizeOfPageSlot >= slotNum) {
			slotNum = 0;
			pageNum = pageNum + 1;
		}
		table->recordEnd = pageNum;
	}

	else {
		int i = 0;
		while (i <= 0) {
			count = 1;
				//slotNum = setSlotNumber(table);
				pageNum = setPageNum(table);
			i++;
		}
	}
	if (true)
			updateRecordDetails(record, pageNum, slotNum);

	pageHandOperations(table, pageNum, slotNum, rel, record,0);
	return RC_OK;
}

RC tableInfoDetailsToFileData(char *name, char *temp,
		recordTableInfo *recordTable) {
	SM_FileHandle fHandle;
	if (!checkTableExists(name)) {
		return RC_TABLE_DOES_NOT_EXIST;
	} else {
		if (openPageFile(name, &fHandle) == RC_OK) {
			char *recordTableString = convertTableString(recordTable, 0);
			writeBlock(0, &fHandle, recordTableString);
			closePageFile(&fHandle);
		}
	}
	return RC_OK;
}

RC createRecord(Record **record, Schema *schema) {
	int i = 0;
	while (i <= 0) {
		*record = (Record *) calloc(1, sizeof(Record));
			(*record)->data = (char *) calloc(1, getRecordSize(schema));

		i++;
	}
	return RC_OK;
}

RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {
	int len = 0, diff = 0;
	char *tempData;
	char *stringValue;
	*value = (Value *) calloc(1, sizeof(Value));
	(*value)->dt = schema->dataTypes[attrNum];
	diff = setPosDifference(schema, attrNum, 0);
	tempData = (record->data) + diff;
	switch (schema->dataTypes[attrNum]) {
	case DT_STRING:
		len  = schema->typeLength[attrNum];
		stringValue = (char *) malloc(len + 1);
		setStringAttr(tempData, stringValue, len , value);
		break;
	case DT_BOOL:
		memcpy(&((*value)->v.boolV), tempData, sizeof(bool));
		break;
	case DT_INT:
		memcpy(&((*value)->v.intV), tempData, sizeof(int));
		break;

	case DT_FLOAT:
		memcpy(&((*value)->v.floatV), tempData, sizeof(float));
		break;
	}
	return RC_OK;
}

RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
	int len = 0, diff=0;
	char *tempData;
	char *stringValue;
	diff = setPosDifference(schema, attrNum, 0);
	 tempData = (record->data) + diff;
	switch (schema->dataTypes[attrNum]) {
	case DT_STRING:
			len = schema->typeLength[attrNum];
			stringValue = (char *) malloc(len);
			stringValue = value->v.stringV;
			memcpy(tempData, stringValue, len);
			break;
	case DT_INT:
			memcpy(tempData, &(value->v.intV), sizeof(int));
		break;
	case DT_FLOAT:
			memcpy(tempData, &((value->v.floatV)), sizeof(float));
		break;
	case DT_BOOL:
			memcpy(tempData, &((value->v.boolV)), sizeof(bool));
		break;
	}
	return RC_OK;
}

expressionSearch *initSearchRecord() {
	expressionSearch *recSearch = NULL;
	if(recSearch==NULL)
		recSearch = (expressionSearch *) calloc(1, sizeof(expressionSearch));
	return recSearch;
}

RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
	int i = 0;
	while (i <= 0) {
		findRecordInSchema(rel, scan, cond);
		i++;
	}
	return RC_OK;
}

RC next(RM_ScanHandle *scan, Record *record) {
	expressionSearch *recSearch = getExprSrchPointer(scan);
	Value *val;
	populScanData(record, recSearch);
	if (getRecord(scan->rel, record->id, record) != RC_RM_NO_MORE_TUPLES) {
		if (record->id.tombS) {
			if (recSearch->curRecordSlot
					== recSearch->numOfSlotsInRelation - 1) {
				updateExprSearch(recSearch, record,0);
				scan->mgmtData = recSearch;
				return next(scan, record);
			} else {
				recSearch->curRecordSlot = recSearch->curRecordSlot + 1;
				scan->mgmtData = recSearch;
				return next(scan, record);
			}
		} else {
			evaluate(scan, record, &val, recSearch);
			if (recSearch->curRecordSlot
					!= recSearch->numOfSlotsInRelation - 1) {
				recSearch->curRecordSlot = recSearch->curRecordSlot + 1;
				scan->mgmtData = recSearch;
			} else {
				updateExprSearch(recSearch, record,0);
				scan->mgmtData = recSearch;
			}
			if (val->v.boolV == 1)
				return RC_OK;
			else
				return next(scan, record);

		}
	} else {
		return RC_RM_NO_MORE_TUPLES;
	}
	return RC_OK;
}

RC deleteRecord(RM_TableData *rel, RID id) {
		recordTableInfo *table = NULL;
		table = getRecordManTabInfo(rel);
		tombStoneMarker *tombstone = NULL;
		tombstone = table->tombStoneHead;
	if (0 >= table->numOfRecords) {
			return RC_RM_NO_MORE_TUPLES;
	}

	if (table->tombStoneHead) {
		while (tombstone->nextDeletedRecord)
			tombstone = tombstone->nextDeletedRecord;
		tombstone->nextDeletedRecord = getTombNode();
		tombstone = tombstone->nextDeletedRecord;
		updateTombNodes(tombstone, id, rel, table);
	} else {
		table->tombStoneHead = getTombNode();
		table->tombStoneHead->nextDeletedRecord = NULL;
		tombstone = table->tombStoneHead;
		updateTombNodes(tombstone, id, rel, table);
	}
	return RC_OK;
}

RC updateRecord(RM_TableData *rel, Record *rec) {
	int pageNum;
	int slotNum;
	BM_PageHandle *ph = NULL;
	ph = getBuffPageHandle();
	recordTableInfo *table = NULL;
	table = getRecordManTabInfo(rel);
	updatePageSlot(&pageNum, &slotNum, rec, 0);
	char *stringRecord = NULL;
	stringRecord = serializeRecord(rec, rel->schema);
	if (pinPage(table->bufferManager, ph, pageNum) == RC_OK)
		strncpy((slotNum * (table->slotSize)) + ph->data, stringRecord,
				strlen(stringRecord));
	updateOperationDetails(rel, table, ph, 0);
	return RC_OK;
}

RC getRecord(RM_TableData *rel, RID id, Record *record) {
	int pageNum, slotNum;
	int tombCount = 0;
	bool tombFlag;
	int tuples;
	BM_PageHandle *ph = NULL;
	ph = getBuffPageHandle();
	recordTableInfo *table = NULL;
	table = getRecordManTabInfo(rel);

	updateDetls(id, &pageNum, &slotNum,0);
	updateRecordInfoDetails(record, &pageNum, &slotNum,0);

	record->id.tombS = false;
	tombFlag = checkIfTombStone(record, rel, id, &pageNum, &slotNum,
			&tombCount);

	if (ph) {
		if (tombFlag == false) {
			tuples = (pageNum - table->recordBegin) * (table->maxSizeOfPageSlot)
					+ slotNum + 1 - tombCount;
			if (table->numOfRecords >= tuples)
				getRecordPageOperDtls(rel, record, table, ph, pageNum, slotNum);
			else {
				free(ph);
				return RC_RM_NO_MORE_TUPLES;
			}
		}
		free(ph);
	}
	return RC_OK;
}

RC closeScan (RM_ScanHandle *scan){
	return RC_OK;
}
