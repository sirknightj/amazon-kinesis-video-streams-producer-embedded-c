/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include <inttypes.h>
#include <string.h>

/* Third party headers */
#include "azure_c_shared_utility/doublylinkedlist.h"
#include "azure_c_shared_utility/lock.h"
#include "azure_c_shared_utility/xlogging.h"

/* Public headers */
#include "kvs/errors.h"
#include "kvs/mkv_generator.h"
#include "kvs/stream.h"

/* Internal headers */
#include "os/allocator.h"

typedef struct DataFrame
{
    DataFrameIn_t xDataFrameIn;

    DLIST_ENTRY xClusterEntry;
    DLIST_ENTRY xDataFrameEntry;

    size_t uMkvHdrLen;
    char *pMkvHdr;
} DataFrame_t;

typedef struct Stream
{
    LOCK_HANDLE xLock;

    char *pMkvEbmlSeg;
    size_t uMkvEbmlSegLen;

    uint64_t uEarliestClusterTimestamp;
    DLIST_ENTRY xClusterPending;
    DLIST_ENTRY xDataFramePending;

    bool bHasVideoTrack;
    bool bHasAudioTrack;
} Stream_t;

static DataFrameHandle prvStreamPop(StreamHandle xStreamHandle, bool bPeek)
{
    Stream_t *pxStream = xStreamHandle;
    DataFrame_t *pxDataFrame = NULL;
    PDLIST_ENTRY pxListHead = NULL;
    PDLIST_ENTRY pxListItem = NULL;

    if (pxStream == NULL)
    {
        LogError("invalid argument");
    }
    else
    {
        if (Lock(pxStream->xLock) != LOCK_OK)
        {
            LogError("Failed to Lock");
        }
        else
        {
            if (DList_IsListEmpty(&(pxStream->xDataFramePending)))
            {
                /* LogInfo("No data frame to pop"); */
            }
            else
            {
                pxListHead = &(pxStream->xDataFramePending);

                if (!bPeek)
                {
                    pxListItem = DList_RemoveHeadList(pxListHead);
                }
                else
                {
                    pxListItem = pxListHead->Flink;
                }
                pxDataFrame = containingRecord(pxListItem, DataFrame_t, xDataFrameEntry);

                if (!bPeek)
                {
                    if (pxDataFrame->xDataFrameIn.xClusterType == MKV_CLUSTER)
                    {
                        pxStream->uEarliestClusterTimestamp = pxDataFrame->xDataFrameIn.uTimestampMs;
                    }
                }
            }

            Unlock(pxStream->xLock);
        }
    }

    return pxDataFrame;
}

StreamHandle Kvs_streamCreate(VideoTrackInfo_t *pVideoTrackInfo, AudioTrackInfo_t *pAudioTrackInfo)
{
    Stream_t *pxStream = NULL;
    MkvHeader_t xMkvHeader = {0};

    if (pVideoTrackInfo == NULL)
    {
        LogError("Invalid argument");
    }
    else if ((pxStream = (Stream_t *)kvsMalloc(sizeof(Stream_t))) == NULL)
    {
        LogError("OOM: pxStream");
    }
    else
    {
        memset(pxStream, 0, sizeof(Stream_t));

        DList_InitializeListHead(&(pxStream->xClusterPending));
        DList_InitializeListHead(&(pxStream->xDataFramePending));

        if (Mkv_initializeHeaders(&xMkvHeader, pVideoTrackInfo, pAudioTrackInfo) != KVS_ERRNO_NONE)
        {
            LogError("Failed to initialize mkv headers");
            kvsFree(pxStream);
            pxStream = NULL;
        }
        else if ((pxStream->xLock = Lock_Init()) == NULL)
        {
            LogError("Failed to initialize lock");
            kvsFree(pxStream);
            pxStream = NULL;
        }
        else
        {
            pxStream->pMkvEbmlSeg = (char *)(xMkvHeader.pHeader);
            pxStream->uMkvEbmlSegLen = (size_t)(xMkvHeader.uHeaderLen);
            pxStream->bHasVideoTrack = true;
            pxStream->bHasAudioTrack = (pAudioTrackInfo == NULL) ? false : true;
        }
    }

    return pxStream;
}

void Kvs_streamTermintate(StreamHandle xStreamHandle)
{
    Stream_t *pxStream = xStreamHandle;

    if (pxStream != NULL)
    {
        kvsFree(pxStream->pMkvEbmlSeg);
        Lock_Deinit(pxStream->xLock);
        kvsFree(pxStream);
    }
}

int Kvs_streamGetMkvEbmlSegHdr(StreamHandle xStreamHandle, uint8_t **ppMkvHeader, size_t *puMkvHeaderLen)
{
    int res = KVS_ERRNO_NONE;
    Stream_t *pxStream = xStreamHandle;

    if (pxStream == NULL || ppMkvHeader == NULL || puMkvHeaderLen == NULL)
    {
        res = KVS_ERROR_INVALID_ARGUMENT;
        LogError("Invalid argument");
    }
    else if (pxStream->pMkvEbmlSeg == NULL || pxStream->uMkvEbmlSegLen == 0)
    {
        res = KVS_ERROR_STREAM_MKV_IS_NOT_INITIALIZED;
        LogError("Mkv EBML and segment are not initialized");
    }
    else
    {
        *ppMkvHeader = (uint8_t *)(pxStream->pMkvEbmlSeg);
        *puMkvHeaderLen = pxStream->uMkvEbmlSegLen;
    }

    return res;
}

DataFrameHandle Kvs_streamAddDataFrame(StreamHandle xStreamHandle, DataFrameIn_t *pxDataFrameIn)
{
    int res = KVS_ERRNO_NONE;
    Stream_t *pxStream = xStreamHandle;
    DataFrame_t *pxDataFrame = NULL;
    size_t uMkvHdrLen = 0;
    DataFrame_t *pxDataFrameCurrent = NULL;
    PDLIST_ENTRY pxListHead = NULL;
    PDLIST_ENTRY pxListItem = NULL;
    bool bListAdded = false;
    bool bNeedCorrectDeltaTimestamp = false;
    bool bCorrectDeltaTimestampStarted = false;
    uint64_t uClusterTimestamp = 0;
    uint16_t uDeltaTimestampMs = 0;

    if (pxStream == NULL || pxDataFrameIn == NULL)
    {
        res = KVS_ERROR_INVALID_ARGUMENT;
        LogError("Invalid argument");
    }
    // Simple block
    else if ((uMkvHdrLen = Mkv_getClusterHdrLen(pxDataFrameIn->xClusterType)) == 0)
    {
        res = KVS_ERROR_INVALID_CLUSTER_HDR_LEN;
        LogError("Invalid cluster len");
    }
    else if ((pxDataFrame = (DataFrame_t *)kvsMalloc(sizeof(DataFrame_t) + uMkvHdrLen)) == NULL)
    {
        res = KVS_ERROR_OUT_OF_MEMORY;
        LogError("OOM: pxDataFrame");
    }
    else if (Lock(pxStream->xLock) != LOCK_OK)
    {
        res = KVS_ERROR_LOCK_ERROR;
        LogError("Failed to Lock");
    }
    else
    {
        memset(pxDataFrame, 0, sizeof(DataFrame_t));
        memcpy(pxDataFrame, pxDataFrameIn, sizeof(DataFrameIn_t));
        DList_InitializeListHead(&(pxDataFrame->xClusterEntry));
        DList_InitializeListHead(&(pxDataFrame->xDataFrameEntry));
        pxDataFrame->uMkvHdrLen = uMkvHdrLen;
        pxDataFrame->pMkvHdr = (char *)pxDataFrame + sizeof(DataFrame_t);
        uClusterTimestamp = pxStream->uEarliestClusterTimestamp;

        pxListHead = &(pxStream->xDataFramePending);
        pxListItem = pxListHead->Flink;
        while (pxListItem != pxListHead)
        {
            pxDataFrameCurrent = containingRecord(pxListItem, DataFrame_t, xDataFrameEntry);
            if (pxDataFrame->xDataFrameIn.uTimestampMs < pxDataFrameCurrent->xDataFrameIn.uTimestampMs ||
                ((pxDataFrame->xDataFrameIn.uTimestampMs == pxDataFrameCurrent->xDataFrameIn.uTimestampMs) && (pxDataFrame->xDataFrameIn.xTrackType == TRACK_VIDEO)))
            {
                DList_InsertTailList(pxListItem, &(pxDataFrame->xDataFrameEntry));
                uDeltaTimestampMs = (uint16_t)(pxDataFrame->xDataFrameIn.uTimestampMs - uClusterTimestamp);
                if (pxDataFrame->xDataFrameIn.xClusterType == MKV_CLUSTER)
                {
                    uDeltaTimestampMs = 0;

                    /* If we insert a cluster head, then the following frames's delta timestamp needs updates. */
                    bNeedCorrectDeltaTimestamp = true;
                }
                bListAdded = true;
                break;
            }
            if (pxDataFrameCurrent->xDataFrameIn.xClusterType == MKV_CLUSTER)
            {
                uClusterTimestamp = pxDataFrameCurrent->xDataFrameIn.uTimestampMs;
            }
            pxListItem = pxListItem->Flink;
        }
        // Create a new one if it doesn't exist
        if (!bListAdded)
        {
            uDeltaTimestampMs = (uint16_t)(pxDataFrame->xDataFrameIn.uTimestampMs - uClusterTimestamp);
            DList_InsertTailList(&(pxStream->xDataFramePending), &(pxDataFrame->xDataFrameEntry));
            bListAdded = true;
        }

        Mkv_initializeClusterHdr(
            (uint8_t *)(pxDataFrame->pMkvHdr),
            pxDataFrame->uMkvHdrLen,
            pxDataFrameIn->xClusterType,
            pxDataFrameIn->uDataLen,
            pxDataFrameIn->xTrackType,
            pxDataFrameIn->bIsKeyFrame,
            pxDataFrameIn->uTimestampMs,
            uDeltaTimestampMs);

        if (bNeedCorrectDeltaTimestamp)
        {
            bCorrectDeltaTimestampStarted = false;
            pxListHead = &(pxStream->xDataFramePending);
            pxListItem = pxListHead->Flink;
            while (pxListItem != pxListHead)
            {
                pxDataFrameCurrent = containingRecord(pxListItem, DataFrame_t, xDataFrameEntry);
                if (pxDataFrameCurrent->xDataFrameIn.xClusterType == MKV_CLUSTER)
                {
                    uClusterTimestamp = pxDataFrameCurrent->xDataFrameIn.uTimestampMs;
                    bCorrectDeltaTimestampStarted = true;
                }
                if (bCorrectDeltaTimestampStarted)
                {
                    uDeltaTimestampMs = (uint16_t)(pxDataFrameCurrent->xDataFrameIn.uTimestampMs - uClusterTimestamp);
                    Mkv_initializeClusterHdr(
                        (uint8_t *)(pxDataFrameCurrent->pMkvHdr),
                        pxDataFrameCurrent->uMkvHdrLen,
                        pxDataFrameCurrent->xDataFrameIn.xClusterType,
                        pxDataFrameCurrent->xDataFrameIn.uDataLen,
                        pxDataFrameCurrent->xDataFrameIn.xTrackType,
                        pxDataFrameCurrent->xDataFrameIn.bIsKeyFrame,
                        pxDataFrameCurrent->xDataFrameIn.uTimestampMs,
                        uDeltaTimestampMs);
                }
                pxListItem = pxListItem->Flink;
            }
        }

        Unlock(pxStream->xLock);
    }

    if (res != KVS_ERRNO_NONE)
    {
        if (pxDataFrame != NULL)
        {
            kvsFree(pxDataFrame);
            pxDataFrame = NULL;
        }
    }

    return pxDataFrame;
}

DataFrameHandle Kvs_streamPop(StreamHandle xStreamHandle)
{
    return prvStreamPop(xStreamHandle, false);
}

DataFrameHandle Kvs_streamPeek(StreamHandle xStreamHandle)
{
    return prvStreamPop(xStreamHandle, true);
}

bool Kvs_streamIsEmpty(StreamHandle xStreamHandle)
{
    bool bRes = true;
    Stream_t *pxStream = xStreamHandle;

    if (pxStream != NULL)
    {
        if (Lock(pxStream->xLock) != LOCK_OK)
        {
            LogError("Failed to Lock");
        }
        else
        {
            if (!DList_IsListEmpty(&(pxStream->xDataFramePending)))
            {
                bRes = false;
            }
            Unlock(pxStream->xLock);
        }
    }

    return bRes;
}

bool Kvs_streamAvailOnTrack(StreamHandle xStreamHandle, TrackType_t xTrackType)
{
    bool bRes = false;
    Stream_t *pxStream = xStreamHandle;
    PDLIST_ENTRY pxListHead = NULL;
    PDLIST_ENTRY pxListItem = NULL;
    DataFrame_t *pxDataFrame = NULL;

    if (pxStream != NULL)
    {
        if (Lock(pxStream->xLock) != LOCK_OK)
        {
            LogError("Failed to Lock");
        }
        else
        {
            pxListHead = &(pxStream->xDataFramePending);
            pxListItem = pxListHead->Flink;
            while (pxListItem != pxListHead)
            {
                pxDataFrame = containingRecord(pxListItem, DataFrame_t, xDataFrameEntry);
                if (pxDataFrame->xDataFrameIn.xTrackType == xTrackType)
                {
                    bRes = true;
                    break;
                }

                pxListItem = pxListItem->Flink;
            }

            Unlock(pxStream->xLock);
        }
    }

    return bRes;
}

int Kvs_streamMemStatTotal(StreamHandle xStreamHandle, size_t *puMemTotal)
{
    int res = KVS_ERRNO_NONE;
    Stream_t *pxStream = xStreamHandle;
    DataFrame_t *pxDataFrame = NULL;
    PDLIST_ENTRY pxListHead = NULL;
    PDLIST_ENTRY pxListItem = NULL;
    size_t uMemTotal = 0;

    if (pxStream == NULL || puMemTotal == NULL)
    {
        res = KVS_ERROR_INVALID_ARGUMENT;
        LogError("Invalid argument");
    }
    else if (Lock(pxStream->xLock) != LOCK_OK)
    {
        res = KVS_ERROR_LOCK_ERROR;
        LogError("Failed to Lock");
    }
    else
    {
        uMemTotal += sizeof(Stream_t) + pxStream->uMkvEbmlSegLen;

        pxListHead = &(pxStream->xDataFramePending);
        pxListItem = pxListHead->Flink;
        while (pxListItem != pxListHead)
        {
            pxDataFrame = containingRecord(pxListItem, DataFrame_t, xDataFrameEntry);
            uMemTotal += pxDataFrame->xDataFrameIn.uDataLen;
            uMemTotal += sizeof(DataFrame_t) + pxDataFrame->uMkvHdrLen;
            pxListItem = pxListItem->Flink;
        }

        *puMemTotal = uMemTotal;
        Unlock(pxStream->xLock);
    }

    return res;
}

int Kvs_dataFrameGetContent(DataFrameHandle xDataFrameHandle, uint8_t **ppMkvHeader, size_t *puMkvHeaderLen, uint8_t **ppData, size_t *puDataLen)
{
    int res = KVS_ERRNO_NONE;
    DataFrame_t *pxDataFrame = xDataFrameHandle;

    if (pxDataFrame == NULL || ppMkvHeader == NULL || puMkvHeaderLen == NULL || ppData == NULL || puDataLen == NULL)
    {
        res = KVS_ERROR_INVALID_ARGUMENT;
        LogError("Invalid argument");
    }
    else
    {
        *ppMkvHeader = (uint8_t *)(pxDataFrame->pMkvHdr);
        *puMkvHeaderLen = pxDataFrame->uMkvHdrLen;
        *ppData = (uint8_t *)(pxDataFrame->xDataFrameIn.pData);
        *puDataLen = pxDataFrame->xDataFrameIn.uDataLen;
    }

    return res;


//    int res = KVS_ERRNO_NONE;
//    DataFrame_t *pxDataFrame = xDataFrameHandle;
//
//    static bool firstClusterSeen = false;
//    static uint32_t clusterCount = 0;
//    static uint32_t tagCounter = 1;
//
//    // Input validation
//    if (pxDataFrame == NULL || ppMkvHeader == NULL ||
//        puMkvHeaderLen == NULL || ppData == NULL || puDataLen == NULL) {
//        LogError("Invalid argument in Kvs_dataFrameGetContent");
//        return KVS_ERROR_INVALID_ARGUMENT;
//    }
//
//    // Track cluster transitions
//    if (pxDataFrame->xDataFrameIn.xClusterType == MKV_CLUSTER) {
//        if (!firstClusterSeen) {
//            firstClusterSeen = true;
//            clusterCount++;
//            LogInfo("First cluster detected");
//        } else {
//            clusterCount++;
//            LogInfo("Cluster #%u detected - Adding tags before cluster", clusterCount);
//
//            // Define tags with exact sizes
//            MkvTag_t tags[1];
//            memset(tags, 0, sizeof(tags));
//
//            const char* tagKey = "TEST_KEY_1D";
//            size_t keyLen = strlen(tagKey);
//
//            char tagValue[32];
//            int valueLen = snprintf(tagValue, sizeof(tagValue), "TEST_VALUE_%uD", tagCounter++);
//            if (valueLen < 0 || valueLen >= sizeof(tagValue)) {
//                LogError("Tag value too long");
//                return KVS_ERROR_INVALID_ARGUMENT;
//            }
//
//            // Copy strings without null terminators for MKV content
//            memcpy(tags[0].key, tagKey, keyLen);
//            memcpy(tags[0].value, tagValue, valueLen);
//
//            // Get the original header information
//            uint8_t *originalHeader = (uint8_t *)pxDataFrame->pMkvHdr;
//            size_t originalHeaderLen = pxDataFrame->uMkvHdrLen;
//
//            // Generate tags header
//            MkvTagsBuffer_t tagsHdr;
//            if (Mkv_initializeTagsHdr(1, tags, &tagsHdr) != KVS_ERRNO_NONE || tagsHdr.buffer == NULL) {
//                LogError("Failed to create MKV tags header");
//                return KVS_ERROR_OUT_OF_MEMORY;
//            }
//
//            // Allocate and create new combined header
//            size_t newHeaderLen = tagsHdr.size + originalHeaderLen;
//            uint8_t *newHeader = (uint8_t *)malloc(newHeaderLen);
//            if (newHeader == NULL) {
//                free(tagsHdr.buffer);
//                LogError("Failed to allocate memory for new header");
//                return KVS_ERROR_OUT_OF_MEMORY;
//            }
//
//            // Combine headers
//            memcpy(newHeader, tagsHdr.buffer, tagsHdr.size);
//            memcpy(newHeader + tagsHdr.size, originalHeader, originalHeaderLen);
//
//            // Clean up tags header buffer
//            free(tagsHdr.buffer);
//
//            // Update the frame's header information
//            pxDataFrame->pMkvHdr = (char *)newHeader;
//            pxDataFrame->uMkvHdrLen = newHeaderLen;
//
//            LogInfo("MKV tags added successfully before cluster #%u. New header size: %zu",
//                    clusterCount, pxDataFrame->uMkvHdrLen);
//
//            // Debug: Print first 64 bytes of the new header
//            LogInfo("New header content (first 64 bytes):");
//            for (size_t i = 0; i < 64 && i < pxDataFrame->uMkvHdrLen; i++) {
//                printf("%02x ", newHeader[i]);
//                if ((i + 1) % 16 == 0) printf("\n");
//            }
//            printf("\n");
//        }
//    }
//
//    // Assign frame content
//    *ppMkvHeader = (uint8_t *)(pxDataFrame->pMkvHdr);
//    *puMkvHeaderLen = pxDataFrame->uMkvHdrLen;
//    *ppData = (uint8_t *)(pxDataFrame->xDataFrameIn.pData);
//    *puDataLen = pxDataFrame->xDataFrameIn.uDataLen;
//
//    return res;
}

void printHexBuffer(const char *label, const uint8_t *buffer, size_t length) {
    printf("%s (size: %zu):\n", label, length);
    for (size_t i = 0; i < length; i++) {
        printf("%02X ", buffer[i]);
        if ((i + 1) % 16 == 0) {
            printf("\n");
        }
    }
    printf("\n");
}

int Kvs_dataFrameAddTags(DataFrameHandle xDataFrameHandle, MkvTag_t* tagsList, size_t tagsListLen, bool endOfStream, uint8_t **ppMkvHeader, size_t *puMkvHeaderLen, uint8_t **ppData, size_t *puDataLen) {
    int res = KVS_ERRNO_NONE;
    DataFrame_t *pxDataFrame = xDataFrameHandle;

    static bool firstClusterSeen = false;
    static uint32_t clusterCount = 0;
    static uint32_t tagCounter = 1;

    // We only add tags at the end
    if (pxDataFrame->xDataFrameIn.xClusterType != MKV_CLUSTER) {
        return KVS_ERRNO_NONE;
    }

    // Input validation
    if (pxDataFrame == NULL || ppMkvHeader == NULL ||
        puMkvHeaderLen == NULL || ppData == NULL || puDataLen == NULL) {
        LogError("Invalid argument in Kvs_dataFrameGetContent");
        return KVS_ERROR_INVALID_ARGUMENT;
    }

    for (size_t i = 0; i < tagsListLen; i++) {
        printf("Adding tag: key=%s, val=%s\n", tagsList[i].key, tagsList[i].value);
    }

    // Track cluster transitions
    if (!firstClusterSeen) {
        firstClusterSeen = true;
        clusterCount++;
        LogInfo("First cluster detected");
    } else {
        clusterCount++;
        LogInfo("Cluster #%u detected - Adding tags before cluster", clusterCount);

        // Get the original header information
        uint8_t *originalHeader = (uint8_t *)pxDataFrame->pMkvHdr;
        size_t originalHeaderLen = pxDataFrame->uMkvHdrLen;

        MkvTag_t* tagsToAdd;
        size_t tagsToAddLen;
        if (endOfStream) {
            MkvTag_t tagsListCopy[tagsListLen + 1];
            memcpy(tagsListCopy, tagsList, tagsListLen * sizeof(MkvTag_t));

            MkvTag_t endOfFragmentTag;
            snprintf(endOfFragmentTag.key, 128, "AWS_KINESISVIDEO_END_OF_FRAGMENT");
            snprintf(endOfFragmentTag.value, 256, "");

            tagsListCopy[tagsListLen] = endOfFragmentTag;

            tagsToAdd = tagsListCopy;
            tagsToAddLen = tagsListLen + 1;
        } else {
            tagsToAdd = tagsList;
            tagsToAddLen = tagsListLen;
        }

        // Generate tags header
        MkvTagsBuffer_t tagsHdr;
        if (Mkv_initializeTagsHdr(tagsToAddLen, tagsToAdd, &tagsHdr) != KVS_ERRNO_NONE || tagsHdr.buffer == NULL) {
            LogError("Failed to create MKV tags header");
            return KVS_ERROR_OUT_OF_MEMORY;
        }

        // Allocate and create new combined header
        size_t newHeaderLen = tagsHdr.size + originalHeaderLen;
        uint8_t *newHeader = (uint8_t *)malloc(newHeaderLen);
        if (newHeader == NULL) {
            free(tagsHdr.buffer);
            LogError("Failed to allocate memory for new header");
            return KVS_ERROR_OUT_OF_MEMORY;
        }

        // Combine headers
        memcpy(newHeader, tagsHdr.buffer, tagsHdr.size);
        memcpy(newHeader + tagsHdr.size, originalHeader, originalHeaderLen);

        // Clean up tags header buffer
        free(tagsHdr.buffer);

        // Update the frame's header information
        pxDataFrame->pMkvHdr = (char *)newHeader;
        pxDataFrame->uMkvHdrLen = newHeaderLen;
    }

    // Assign frame content
    *ppMkvHeader = (uint8_t *)(pxDataFrame->pMkvHdr);
    *puMkvHeaderLen = pxDataFrame->uMkvHdrLen;
    *ppData = (uint8_t *)(pxDataFrame->xDataFrameIn.pData);
    *puDataLen = pxDataFrame->xDataFrameIn.uDataLen;

    return res;
}

int addTagsAtEnd(DataFrameHandle xDataFrameHandle, MkvTag_t* tagsList, size_t tagsListLen,
                 uint8_t **ppMkvHeader, size_t *puMkvHeaderLen, uint8_t **ppData, size_t *puDataLen) {
    if (xDataFrameHandle == NULL || tagsList == NULL || ppMkvHeader == NULL || puMkvHeaderLen == NULL ||
        ppData == NULL || puDataLen == NULL) {
        LogError("Invalid arguments in addTagsAtEnd");
        return KVS_ERROR_INVALID_ARGUMENT;
    }

    static bool calledAlready = false;
    if (calledAlready) {
        return KVS_ERRNO_NONE;
    }
    calledAlready = true;

    DataFrame_t *pxDataFrame = xDataFrameHandle;
    LogInfo("Adding final MKV tags at the end of the stream");

    printHexBuffer("Buffer before modification", (uint8_t *)pxDataFrame->xDataFrameIn.pData, pxDataFrame->xDataFrameIn.uDataLen);

    // Allocate space for original tags + end-of-fragment tag
    size_t extendedTagsLen = tagsListLen + 1;
    MkvTag_t *extendedTagsList = (MkvTag_t *)malloc(extendedTagsLen * sizeof(MkvTag_t));
    if (extendedTagsList == NULL) {
        LogError("Failed to allocate memory for extended tags list");
        return KVS_ERROR_OUT_OF_MEMORY;
    }

    // Copy original tags
    memcpy(extendedTagsList, tagsList, tagsListLen * sizeof(MkvTag_t));

    // Add "AWS_KINESISVIDEO_END_OF_FRAGMENT" tag
    MkvTag_t endOfFragmentTag = {0};
    strncpy(endOfFragmentTag.key, "AWS_KINESISVIDEO_END_OF_FRAGMENT", sizeof(endOfFragmentTag.key) - 1);
    endOfFragmentTag.key[sizeof(endOfFragmentTag.key) - 1] = '\0';
    endOfFragmentTag.value[0] = '\0';

    extendedTagsList[tagsListLen] = endOfFragmentTag;

    // Generate final MKV tags header
    MkvTagsBuffer_t tagsHdr;
    if (Mkv_initializeTagsHdr(extendedTagsLen, extendedTagsList, &tagsHdr) != KVS_ERRNO_NONE || tagsHdr.buffer == NULL) {
        LogError("Failed to create MKV tags header");
        free(extendedTagsList);
        return KVS_ERROR_OUT_OF_MEMORY;
    }

    // Allocate space for the new data with final tags at the end
    size_t newDataLen = pxDataFrame->xDataFrameIn.uDataLen + tagsHdr.size;
    uint8_t *newData = (uint8_t *)malloc(newDataLen);
    if (newData == NULL) {
        free(tagsHdr.buffer);
        free(extendedTagsList);
        LogError("Failed to allocate memory for new data buffer");
        return KVS_ERROR_OUT_OF_MEMORY;
    }

    // Copy original data and append the final tags
    memcpy(newData, pxDataFrame->xDataFrameIn.pData, pxDataFrame->xDataFrameIn.uDataLen);
    memcpy(newData + pxDataFrame->xDataFrameIn.uDataLen, tagsHdr.buffer, tagsHdr.size);

    printHexBuffer("Buffer after modification", newData, newDataLen);

    // Free old data buffer before replacing it
    free(pxDataFrame->xDataFrameIn.pData);

    // Update frame with new data
    pxDataFrame->xDataFrameIn.pData = (char *)newData;
    pxDataFrame->xDataFrameIn.uDataLen = newDataLen;

    // Assign the output parameters
    *ppMkvHeader = (uint8_t *)(pxDataFrame->pMkvHdr);
    *puMkvHeaderLen = pxDataFrame->uMkvHdrLen;
    *ppData = (uint8_t *)(pxDataFrame->xDataFrameIn.pData);
    *puDataLen = pxDataFrame->xDataFrameIn.uDataLen;

    // Clean up
    free(tagsHdr.buffer);
    free(extendedTagsList);

    LogInfo("Final MKV tags added at the end. New data size: %zu", newDataLen);
    return KVS_ERRNO_NONE;
}


void Kvs_dataFrameTerminate(DataFrameHandle xDataFrameHandle)
{
    DataFrame_t *pxDataFrame = xDataFrameHandle;

    if (pxDataFrame != NULL)
    {
        kvsFree(pxDataFrame);
    }
}
