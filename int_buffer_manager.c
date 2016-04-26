/*
 * InitBufferManager.c
 *
 *
 *      Author: Smriti
 */



#include <stdlib.h>

#include <string.h>
#include "buffer_mgr.h"
#include "buffer_man_assign.h"
#include "storage_mgr.h"
#include "init_buffer_manager.h"


bufferPoolDetails *initbufferPoolDetails(const int numPages,
		SM_FileHandle fileHandle) {
	bufferPoolDetails *buff = malloc(sizeof(SM_PageHandle) * PAGE_SIZE);
	if(PAGE_SIZE!=-100){
		buff->maxNumOfFrames = numPages;
		buff->filePointer = fileHandle;
		buff->numOfReads = 0;
		buff->numOfWrites = 0;
		buff->numOfFrames = 0;
		buff->count = 0;
	}

	if(PAGE_SIZE!=-100){
		memset(buff->maplrupage, NO_PAGE,
		LRUFRAMESIZE * sizeof(int));
		memset(buff->mapPageFrame, NO_PAGE,
		MAXPAGESIZE * sizeof(int));
		memset(buff->mapPage, NO_PAGE,
		MAXFRAMESIZE * sizeof(int));
		memset(buff->mapdirtbits, NO_PAGE,
		MAXFRAMESIZE * sizeof(bool));
		memset(buff->mapFixedCount, NO_PAGE,
		MAXFRAMESIZE * sizeof(int));
	}
	return buff;
}

pageFrameDetails *getNewNode(){

	pageFrameDetails *node = malloc(sizeof(SM_PageHandle) * PAGE_SIZE);
	memset(node, 0x0, sizeof(SM_PageHandle) * PAGE_SIZE);
	int numVal=0;
	node->isDirty = false;
	node->numOfPins = numVal;
	node->fixedCount = numVal;
	node->filledBlocks = numVal;
	node->pageFrameNum = numVal;
	if(PAGE_SIZE!=-100)
	node->next = NULL;
	node->previous = NULL;
	node->pageNum = NO_PAGE;
	if(PAGE_SIZE!=-100)
	node->data = (char *) malloc(sizeof(SM_PageHandle) * PAGE_SIZE);
	memset(node->data, 0x0, sizeof(SM_PageHandle) * PAGE_SIZE);
	return node;
}
