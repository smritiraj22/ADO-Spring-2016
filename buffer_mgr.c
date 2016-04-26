#include <stdlib.h>
#include <string.h>

#include"dberror.h"
#include "buffer_mgr.h"
#include "buffer_man_assign.h"
#include "init_buffer_manager.h"
#include "storage_mgr.h"

int countofRecords=0;

pageFrameDetails *findNode(BM_BufferPool * const bm, BM_PageHandle * const page,
		const PageNumber pageNum);

RC initBufferPool(BM_BufferPool * const bm, const char * const pageFileName,
		const int numPages, ReplacementStrategy strategy, void *stratData) {
	SM_FileHandle fh;
	int i;
	if (openPageFile((char *) pageFileName, &fh) == RC_OK) {
		return RC_FILE_NOT_FOUND;

	} else {
		bufferPoolDetails *buffPoolDetails = initbufferPoolDetails(numPages,
				fh);

		bm->strategy = strategy;
		bm->pageFile = (char *) pageFileName;
		bm->numPages = numPages;

		pageFrameDetails *node = getNewNode();
		buffPoolDetails->begin = buffPoolDetails->next = node;
		buffPoolDetails->begin->pageFrameNum = 0;

		for (i = 1; i < numPages; i++) {
			int j=i;
			pageFrameDetails *newNode = getNewNode();
			buffPoolDetails->next->next = newNode;
			buffPoolDetails->next->next->previous = buffPoolDetails->next;
			buffPoolDetails->next = buffPoolDetails->next->next;
			buffPoolDetails->next->pageFrameNum = j;
		}
		buffPoolDetails->end = buffPoolDetails->next;
		bm->mgmtData = buffPoolDetails;
		return RC_OK;
	}

}

bufferPoolDetails *getMgmtInfoDetails(BM_BufferPool * const bm) {
	if (!bm) {
		return NULL;
	} else {
		bufferPoolDetails *mgmtInfo = (bufferPoolDetails*) bm->mgmtData;
		return mgmtInfo;
	}

}

int getNumReadOperations(BM_BufferPool * const bm) {
	int reads = 0;
	if (!bm) {
		return reads;
	} else {
		bufferPoolDetails *mgmtInfo = getMgmtInfoDetails(bm);
		reads = mgmtInfo->numOfReads;
		return reads;
	}

}

int *getFixCountDetails(BM_BufferPool * const bm) {
	if (!bm) {
		return NULL;
	} else {
		pageFrameDetails *pageFrameDetails = NULL;
		bufferPoolDetails *mgmtInfo = getMgmtInfoDetails(bm);

		for (pageFrameDetails = mgmtInfo->begin; pageFrameDetails != NULL;
				pageFrameDetails = pageFrameDetails->next)
			(mgmtInfo->mapFixedCount)[pageFrameDetails->pageFrameNum] =
					pageFrameDetails->fixedCount;
		free(pageFrameDetails);
		return mgmtInfo->mapFixedCount;
	}

}

int getNumWriteOperations(BM_BufferPool * const bm) {
	int reads = 0;
	if (!bm) {
		return reads;
	} else {
		bufferPoolDetails *mgmtInfo = getMgmtInfoDetails(bm);
		reads = mgmtInfo->numOfWrites;
		return reads;
	}

}

bool *getAllDirtyFlags(BM_BufferPool * const bm) {
	if (!bm) {
		return NULL;
	} else {
		pageFrameDetails *pageFrameDetails = NULL;
		bufferPoolDetails *mgmtInfo = getMgmtInfoDetails(bm);
		for (pageFrameDetails = mgmtInfo->begin; pageFrameDetails != NULL;
				pageFrameDetails = pageFrameDetails->next)
			(mgmtInfo->mapdirtbits)[pageFrameDetails->pageFrameNum] =
					pageFrameDetails->isDirty;
		free(pageFrameDetails);
		return mgmtInfo->mapdirtbits;
	}

}

RC markDirty(BM_BufferPool * const bm, BM_PageHandle * const page) {
	if (!bm) {
		return RC_BUFFER_NOT_INITIALIZED;
	} else {
		pageFrameDetails *pageFrameDetls = NULL;
		bufferPoolDetails *mgmtInfo = getMgmtInfoDetails(bm);
		for (pageFrameDetls = mgmtInfo->begin; pageFrameDetls != NULL;
				pageFrameDetls = pageFrameDetls->next) {
			if (pageFrameDetls->pageNum == page->pageNum)
				pageFrameDetls->isDirty = true;
		}
		free(pageFrameDetls);
		return RC_OK;
	}
}

RC unpinPage(BM_BufferPool * const bm, BM_PageHandle * const page) {
	if (!bm) {
		return RC_BUFFER_NOT_INITIALIZED;
	} else {
		pageFrameDetails *pageFrameDtls = NULL;
		bufferPoolDetails *mgmtInfo = getMgmtInfoDetails(bm);
		for (pageFrameDtls = mgmtInfo->begin; pageFrameDtls != NULL;
				pageFrameDtls = pageFrameDtls->next) {
			if (pageFrameDtls->pageNum == page->pageNum
					&& pageFrameDtls->fixedCount > 0)
				pageFrameDtls->fixedCount = pageFrameDtls->fixedCount - 1;
		}
		free(pageFrameDtls);
		return RC_OK;
	}

}

RC pinPage(BM_BufferPool * const bm, BM_PageHandle * const page,
		const PageNumber pageNum) {

	if (!bm) {
		return RC_BUFFER_NOT_INITIALIZED;
	} else {
		switch (bm->strategy) {
		case RS_FIFO:
			fifo(bm, page, pageNum);
			break;
		case RS_LRU:
			lru(bm, page, pageNum);
			break;
		case RS_CLOCK:
			break;
		case RS_LFU:
			break;
		case RS_LRU_K:
			break;
		default:
			break;
		}

		return RC_OK;
	}

}

RC shutdownBufferPool(BM_BufferPool * const bm) {

	if (!bm) {
		return RC_BUFFER_NOT_INITIALIZED;
	} else {
		forceFlushPool(bm);
		bufferPoolDetails *mgmtInfo = getMgmtInfoDetails(bm);
		pageFrameDetails *pageFrameDtls = mgmtInfo->begin;

		while (pageFrameDtls != NULL) {
			free(mgmtInfo->begin->data);
			free(mgmtInfo->begin);
			pageFrameDtls = pageFrameDtls->next;
			mgmtInfo->begin = pageFrameDtls;
		}

		mgmtInfo->begin = NULL;
		mgmtInfo->end = NULL;
		free(pageFrameDtls);
		return RC_OK;
	}

}

RC forcePage(BM_BufferPool * const bm, BM_PageHandle * const page) {
	SM_FileHandle fileHandle;
	pageFrameDetails *pageFrameDtls = NULL;

	if (!bm) {
		return RC_BUFFER_NOT_INITIALIZED;
	} else {
		bufferPoolDetails *mgmtInfo = getMgmtInfoDetails(bm);

		if (openPageFile((char *) (bm->pageFile), &fileHandle) != RC_OK) {
			return RC_FILE_NOT_FOUND;
		} else {

			for (pageFrameDtls = mgmtInfo->begin; pageFrameDtls != NULL;
					pageFrameDtls = pageFrameDtls->next) {
				if (pageFrameDtls->pageNum
						== page->pageNum&& pageFrameDtls->isDirty==true) {
					if (writeBlock(pageFrameDtls->pageNum,
							&(mgmtInfo->filePointer),
							pageFrameDtls->data) != RC_OK) {
						return RC_WRITE_FAILED;
					} else {
						pageFrameDtls->isDirty = false;
						mgmtInfo->numOfWrites = mgmtInfo->numOfWrites + 1;
					}
				}
			}
			free(pageFrameDtls);

		}

	}
	return RC_OK;
}

RC forceFlushPool(BM_BufferPool * const bm) {

	SM_FileHandle fh;
	pageFrameDetails *pageFrameDetails = NULL;

	if (!bm) {
		return RC_BUFFER_NOT_INITIALIZED;
	} else {
		bufferPoolDetails *mgmtInfo = getMgmtInfoDetails(bm);

		if (openPageFile((char *) (bm->pageFile), &fh) != RC_OK) {
			return RC_FILE_NOT_FOUND;
		} else {
			for (pageFrameDetails = mgmtInfo->begin; pageFrameDetails != NULL;
					pageFrameDetails = pageFrameDetails->next) {
				if (pageFrameDetails->isDirty == true) {
					if (writeBlock(pageFrameDetails->pageNum, &fh,
							pageFrameDetails->data) != RC_OK) {
						return RC_WRITE_FAILED;
					} else {
						pageFrameDetails->isDirty = false;
						mgmtInfo->numOfWrites = mgmtInfo->numOfWrites + 1;

					}
				}
			}

		}

	}
	return RC_OK;
}

PageNumber *getFrameData(BM_BufferPool * const bm) {
	bufferPoolDetails *mgmtInfo = getMgmtInfoDetails(bm);
	PageNumber *p = mgmtInfo->mapPage;
	return p;
}

pageFrameDetails *locateNodeFromMemory(BM_PageHandle * const page,
		const PageNumber pageNum, BM_BufferPool * const bm) {
	pageFrameDetails *pageFrameDtls;
	if (!bm) {
		return NULL;
	} else {

		bufferPoolDetails *mgmtInfo = getMgmtInfoDetails(bm);
		if ((mgmtInfo->mapPageFrame)[pageNum] != NO_PAGE) {
			if (((pageFrameDtls = findNode(bm, page, pageNum)) == NULL)) {
				//do nothing
			} else {
				pageFrameDtls->fixedCount = pageFrameDtls->fixedCount + 1;
				page->pageNum = pageNum;
				page->data = pageFrameDtls->data;
				return pageFrameDtls;

			}
		}

	}
	return NULL;
}

pageFrameDetails *findNode(BM_BufferPool * const bm, BM_PageHandle * const page,
		const PageNumber pageNum) {
	pageFrameDetails *pageFrameDtls = NULL;
	if (!bm) {
		return NULL;
	} else {
		bufferPoolDetails *mgmtInfo = getMgmtInfoDetails(bm);
		for (pageFrameDtls = mgmtInfo->begin; pageFrameDtls != NULL;
				pageFrameDtls = pageFrameDtls->next) {
			if (pageFrameDtls->pageNum == pageNum)
				return pageFrameDtls;
		}
		return NULL;
	}

}

RC updatePage(BM_BufferPool * const bm, BM_PageHandle * const page,
		pageFrameDetails *node, const PageNumber pageNum) {

	SM_FileHandle fh;
	if (!bm) {
		return RC_BUFFER_NOT_INITIALIZED;
	} else {
		bufferPoolDetails *mgmtInfo = getMgmtInfoDetails(bm);

		if (openPageFile((char *) (bm->pageFile), &fh) == RC_OK) {

			if (node->isDirty == true) {
				ensureCapacity(pageNum, &fh);
				if ((writeBlock(node->pageNum, &fh, node->data)) == RC_OK) {
					mgmtInfo->numOfWrites = mgmtInfo->numOfWrites + 1;
				}
			}

			ensureCapacity(pageNum, &fh);

			if (readBlock(pageNum, &fh, node->data) == RC_OK) {
				(mgmtInfo->mapPageFrame)[node->pageNum] = NO_PAGE;
				node->pageNum = pageNum;
				(mgmtInfo->mapPageFrame)[node->pageNum] = node->pageFrameNum;
				(mgmtInfo->mapPageFrame)[node->pageFrameNum] = node->pageNum;
				int num=1;
				bool flag= false;
				node->isDirty = flag;
				node->fixedCount = num;
				page->pageNum = pageNum;
				page->data = node->data;
				mgmtInfo->numOfReads = mgmtInfo->numOfReads + 1;
			}
		}

	}
	return RC_OK;
}

RC lru(BM_BufferPool * const bm, BM_PageHandle * const page,
		const PageNumber pageNum) {
	pageFrameDetails *pageFrameDtls = NULL;
	int j = 0;
	if (!bm) {
		return RC_BUFFER_NOT_INITIALIZED;
	} else {

		bufferPoolDetails *mgmtInfo = getMgmtInfoDetails(bm);
		if ((pageFrameDtls = locateNodeFromMemory(page, pageNum, bm)) == NULL) {
			return RC_CANT_LOCATE_NODE;
		} else {
			updateNode(bm, pageFrameDtls, pageNum);
			return RC_OK;
		}

		if (mgmtInfo->maxNumOfFrames > mgmtInfo->numOfFrames) {
			pageFrameDtls = mgmtInfo->begin;
			while (j < mgmtInfo->numOfFrames) {
				pageFrameDtls = pageFrameDtls->next;
				j++;
			}
			mgmtInfo->numOfFrames = mgmtInfo->numOfFrames + 1;

			if (updatePageFrame(bm, page, pageFrameDtls, pageNum) != RC_OK) {
				return RC_UPDATE_PAGE_FAILED;
			} else {
				updateNode(bm, pageFrameDtls, pageNum);
			}

		} else {
			pageFrameDtls = mgmtInfo->end;
			for (; pageFrameDtls && pageFrameDtls->fixedCount != 0;)
				pageFrameDtls = pageFrameDtls->previous;

			if (pageFrameDtls) {
				if (updatePageFrame(bm, page, pageFrameDtls, pageNum) != RC_OK) {
					return RC_UPDATE_PAGE_FAILED;
				} else {
					updateNode(bm, pageFrameDtls, pageNum);
				}
			} else {
				return RC_NO_MORE_EMPTY_FRAME;
			}
		}

	}
	return RC_OK;
}

void updateNode(BM_BufferPool * const bm, pageFrameDetails *node,
		const PageNumber pageNum) {

	if (!bm) {
		// do nothing
	} else {
		bufferPoolDetails *mgmtInfo = getMgmtInfoDetails(bm);
		pageFrameDetails *header = mgmtInfo->begin;

		if (node == header)
			return;

		if (node != mgmtInfo->end) {
			node->previous->next = node->next;
			node->next->previous = node->previous;
			markTravrsedRecord();
			header->previous = node;
			node->next = header;
			markTravrsedRecord();
			mgmtInfo->begin = node;
			node->previous = NULL;
			markTravrsedRecord();
			mgmtInfo->begin->previous = NULL;
			markTravrsedRecord();
			mgmtInfo->begin = node;
			mgmtInfo->begin->previous = NULL;
			markTravrsedRecord();
		} else {
			pageFrameDetails *tail = mgmtInfo->end;
			pageFrameDetails *pageFrameDtls = tail->previous;
			markTravrsedRecord();
			mgmtInfo->end = pageFrameDtls;
			pageFrameDtls->next = NULL;
			markTravrsedRecord();

			header->previous = node;
			node->next = header;

			mgmtInfo->begin = node;
			node->previous = NULL;
			markTravrsedRecord();
			mgmtInfo->begin->previous = NULL;
			markTravrsedRecord();
			mgmtInfo->begin = node;
			mgmtInfo->begin->previous = NULL;
			markTravrsedRecord();
		}

	}
}

void markTravrsedRecord(){
	countofRecords++;
}

RC updatePageFrame(BM_BufferPool * const bm, BM_PageHandle * const page,
		pageFrameDetails *node, const PageNumber pageNum) {
	if (updatePage(bm, page, node, pageNum) != RC_OK) {
		return RC_UPDATE_PAGE_FAILED;
	} else {
		return RC_OK;
	}
}

RC fifo(BM_BufferPool * const bm, BM_PageHandle * const page,
		const PageNumber pageNum) {
	RC flag = RC_OK;
	int j = 0;
	if (!bm) {
		return RC_BUFFER_NOT_INITIALIZED;
	} else {
		pageFrameDetails *pageFrameDetails = NULL;
		bufferPoolDetails *mgmtInfo = getMgmtInfoDetails(bm);
		if ((pageFrameDetails = (locateNodeFromMemory(page, pageNum, bm)))
				== NULL)
			return RC_NOT_OK;
		if ((mgmtInfo->maxNumOfFrames > mgmtInfo->numOfFrames)) {
			pageFrameDetails = mgmtInfo->begin;

			while (j < mgmtInfo->numOfFrames) {
				pageFrameDetails = pageFrameDetails->next;
				j++;
			}
			mgmtInfo->numOfFrames = mgmtInfo->numOfFrames + 1;
			updateNode(bm, pageFrameDetails, pageNum);

			updatePageFrame(bm, page, pageFrameDetails, pageNum);
			return RC_OK;
		} else {
			pageFrameDetails = mgmtInfo->end;
			while (pageFrameDetails && pageFrameDetails->fixedCount != 0)
				pageFrameDetails = pageFrameDetails->previous;
			updateNode(bm, pageFrameDetails, pageNum);
			updatePageFrame(bm, page, pageFrameDetails, pageNum);
			return RC_OK;
		}

	}

}


