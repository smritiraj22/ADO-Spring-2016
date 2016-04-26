/*
 * record_mgr_assign.h
 *
 *  Created on: Apr 23, 2016
 *      Author: Smriti
 */

#ifndef RECORD_MGR_ASSIGN_H_
#define RECORD_MGR_ASSIGN_H_

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
#include "buffer_man_assign.h"
#include "init_buffer_manager.h"


typedef struct tombStoneMarker {
	RID recordID;
	struct tombStoneMarker *nextDeletedRecord;
} tombStoneMarker;

typedef struct recordTableInfo {

	int sizeOfSchema;
	int slotSize;

	int recordBegin;
	int recordEnd;

	int numOfRecords;
	int maxSizeOfPageSlot;

	BM_BufferPool *bufferManager;
	int tombStoneListSize;
	int recordsInTombStone;
	tombStoneMarker *tombStoneHead;

	int deleteNodes;

} recordTableInfo;

typedef struct StringRecordConverter {
	int sizeofString;
	char *data;
	int sizeOfBuffer;
} StringRecordConverter;




typedef struct expressionSearch {
	Expr *condition;

	int numOfPagesInRelation;
	int numOfSlotsInRelation;

	int curRecordSlot;
	int currRecordPage;

	int slot;
	int page;
} expressionSearch;


#endif /* ASSIGN4_ASSIGN4_RECORD_MGR_ASSIGN_H_ */
