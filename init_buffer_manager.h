/*
 * InitBufferManager.h
 *
 *      Author: Smriti
 */

#ifndef INIT_BUFFER_MANAGER_H_
#define INIT_BUFFER_MANAGER_H_


#include <stdlib.h>
#include <string.h>
#include "dberror.h"
#include "storage_mgr.h"
#include "buffer_man_assign.h"

extern bufferPoolDetails *initbufferPoolDetails(const int numPages,SM_FileHandle fileHandle);
extern pageFrameDetails *getNewNode();

#endif /* ASSIGN4_ASSIGN4_INIT_BUFFER_MANAGER_H_ */
