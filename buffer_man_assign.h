/*
 * buffer_man_assign.h
 *
 *      Author: Smriti
 */

#ifndef BUFFER_MAN_ASSIGN_H_
#define BUFFER_MAN_ASSIGN_H_

#include <stdlib.h>

#include <string.h>
#include "buffer_mgr.h"
#include "buffer_man_assign.h"
#include "storage_mgr.h"
//#include "init_buffer_manager.h"

#define LRUFRAMESIZE 100
#define LFUFRAMESIZE 100
#define LRU_K_FRAMESIZE 100
#define FIFOFRAMESIZE 100
#define LIFOFRAMESIZE 100
#define MAXFRAMESIZE 100
#define MAXPAGESIZE 7000

typedef struct bufferPoolDetails{
	SM_FileHandle filePointer;
	struct pageFrameDetails *begin;
	struct pageFrameDetails *next;
	struct pageFrameDetails *end;
	int mapPageFrame[MAXPAGESIZE];
	int mapPage[MAXFRAMESIZE];
	int mapFixedCount[MAXFRAMESIZE];
	int mapfifopage[FIFOFRAMESIZE];
	int maplifopage[LIFOFRAMESIZE];
	int maplrupage[LRUFRAMESIZE];
	int maplfupage[LFUFRAMESIZE];
	int maplrukpage[LRU_K_FRAMESIZE];
	bool mapdirtbits[MAXFRAMESIZE];
	int numOfReads;
	int numOfWrites;
	int numOfFrames;
	int maxNumOfFrames;
	int count;
}bufferPoolDetails;


typedef struct pageFrameDetails{
	struct pageFrameDetails *previous;
	struct pageFrameDetails *next;
	int pageNum;
	int pageFrameNum;
	bool isDirty;
	int fixedCount;
	char *data;
	int numOfPins;
	int filledBlocks;
}pageFrameDetails;



#endif /* ASSIGN4_ASSIGN4_BUFFER_MAN_ASSIGN_H_ */
