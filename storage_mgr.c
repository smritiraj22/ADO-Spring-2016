#include<stdio.h>
#include<stdlib.h>
#include<string.h>

#include"storage_mgr.h" 
#include"dberror.h"

FILE *file;
int block;

RC createPageFile(char *fileName) {
	char *first = (char *) (malloc(PAGE_SIZE));
	char *total = (char *) (malloc(PAGE_SIZE));
	int totalSize;
	strcat(total, "1\t");
	file = fopen(fileName, "w");
	totalSize = fwrite(total, sizeof(char), PAGE_SIZE, file);
	totalSize = fwrite(first, sizeof(char), PAGE_SIZE, file);
	if (fclose(file) == 0)
		return RC_OK;
	else
		return RC_NOT_OK;
}

RC openPageFile(char *fileName, SM_FileHandle *fHandle) {

	char *string = (char *) malloc(PAGE_SIZE);
	FILE *filedata;
	if (fHandle == NULL || fHandle->fileName == NULL
			|| fHandle->mgmtInfo == NULL || fHandle->totalNumPages < 0
			|| fHandle->curPagePos < 0) {
		return RC_FILE_HANDLE_NOT_INIT;
	}
	filedata = fopen(fileName, "r+");
	if (!filedata) {
		return RC_FILE_NOT_FOUND;
	} else {
		int pos=0;
		fgets(string, PAGE_SIZE, filedata);
		fHandle->curPagePos = pos;
		fHandle->mgmtInfo = filedata;
		fHandle->fileName = fileName;
		string = strtok(string, "\t");
		fHandle->totalNumPages = atoi(string);

		return RC_OK;
	}

}

RC destroyPageFile(char *fileName) {
	remove(fileName);
	return RC_OK;
}

RC closePageFile(SM_FileHandle *fHandle) {
	if (!file) {
		return RC_FILE_NOT_FOUND;
	}
	fclose(file);
	file = NULL;
	return RC_OK;
}

RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	if (fHandle == NULL || fHandle->fileName == NULL
			|| fHandle->mgmtInfo == NULL || fHandle->totalNumPages < 0
			|| fHandle->curPagePos < 0) {
		return RC_FILE_HANDLE_NOT_INIT;
	} else {

		if ((fHandle->totalNumPages >= 0) && pageNum >= 0) {
			int currentPageNum = pageNum++;
			fseek(fHandle->mgmtInfo,
					(((currentPageNum) * PAGE_SIZE)) * sizeof(char), SEEK_SET);
			fread(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
			fHandle->curPagePos = currentPageNum;

		} else {
			return RC_READ_NON_EXISTING_PAGE;
		}
	}
	return RC_OK;;
}

RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	return readBlock(0, fHandle, memPage);
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	if (fHandle == NULL || fHandle->fileName == NULL
			|| fHandle->mgmtInfo == NULL || fHandle->totalNumPages < 0
			|| fHandle->curPagePos < 0) {
		return RC_FILE_HANDLE_NOT_INIT;
	}
	block = fHandle->curPagePos - 1;
	return readBlock(block, fHandle, memPage);
}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	if (fHandle == NULL || fHandle->fileName == NULL
			|| fHandle->mgmtInfo == NULL || fHandle->totalNumPages < 0
			|| fHandle->curPagePos < 0) {
		return RC_FILE_HANDLE_NOT_INIT;
	}
	block = fHandle->curPagePos;
	return readBlock(block, fHandle, memPage);
}

RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	if (fHandle == NULL || fHandle->fileName == NULL
			|| fHandle->mgmtInfo == NULL || fHandle->totalNumPages < 0
			|| fHandle->curPagePos < 0) {
		return RC_FILE_HANDLE_NOT_INIT;
	}
	block = fHandle->curPagePos + 1;
	return readBlock(block, fHandle, memPage);
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	block = fHandle->totalNumPages - 1;
	return readBlock(block, fHandle, memPage);
}

RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	if (fHandle == NULL || fHandle->fileName == NULL
			|| fHandle->mgmtInfo == NULL || fHandle->totalNumPages < 0
			|| fHandle->curPagePos < 0) {
		return RC_FILE_HANDLE_NOT_INIT;
	} else {
		if ((pageNum <= fHandle->totalNumPages) && pageNum >= 0) {
			int cuurPage = pageNum + 1;
			fseek(fHandle->mgmtInfo, ((cuurPage) * PAGE_SIZE), SEEK_SET);
			fwrite(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
			fHandle->curPagePos = cuurPage;
		}
	}
	return RC_OK;
}

RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	if (fHandle == NULL || fHandle->fileName == NULL
			|| fHandle->mgmtInfo == NULL || fHandle->totalNumPages < 0
			|| fHandle->curPagePos < 0) {
		return RC_FILE_HANDLE_NOT_INIT;
	}
	writeBlock(fHandle->curPagePos, fHandle, memPage);
	return RC_OK;
}

RC appendEmptyBlock(SM_FileHandle *fHandle) {
	int totalBytes;
	FILE *file = NULL;

	if (fHandle == NULL || fHandle->fileName == NULL
			|| fHandle->mgmtInfo == NULL || fHandle->totalNumPages < 0
			|| fHandle->curPagePos < 0) {
		return RC_FILE_HANDLE_NOT_INIT;
	} else {
		SM_PageHandle ph = (char *) malloc(PAGE_SIZE);
		int pageNum = fHandle->totalNumPages + 1;
		fseek(fHandle->mgmtInfo, ((pageNum) * PAGE_SIZE),
		SEEK_END);
		fwrite(ph, PAGE_SIZE, sizeof(char), file);
		fHandle->totalNumPages = pageNum;
		fHandle->curPagePos = pageNum;
		rewind(fHandle->mgmtInfo);
		fprintf(fHandle->mgmtInfo, "%d\n", pageNum);
		pageNum = pageNum + 1;
		fseek(fHandle->mgmtInfo, (pageNum) * PAGE_SIZE,
		SEEK_SET);
		free(ph);
	}
	return RC_OK;
}

RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {

	if (fHandle == NULL || fHandle->fileName == NULL
			|| fHandle->mgmtInfo == NULL || fHandle->totalNumPages < 0
			|| fHandle->curPagePos < 0) {
		return RC_FILE_HANDLE_NOT_INIT;
	} else {
		if (fHandle->totalNumPages < numberOfPages) {
			int diff = numberOfPages - fHandle->totalNumPages;
			int i = 0;
			while (i < diff) {
				appendEmptyBlock(fHandle);
				i++;
			}
		}
	}
	return RC_OK;
}
