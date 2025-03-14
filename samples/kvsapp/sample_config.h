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

#ifndef SAMPLE_CONFIG_H
#define SAMPLE_CONFIG_H

#include "kvs/mkv_generator.h"

/* KVS general configuration */
#define AWS_ACCESS_KEY                  "*"
#define AWS_SECRET_KEY                  "*"
/* Only for AK/SK + STS case */
#define AWS_SESSION_TOKEN               ""

/* KVS stream configuration */
#define KVS_STREAM_NAME                 "kvs_example_camera_stream"
#define AWS_KVS_REGION                  "us-west-2"
#define AWS_KVS_SERVICE                 "kinesisvideo"
#define AWS_KVS_HOST                    AWS_KVS_SERVICE "." AWS_KVS_REGION ".amazonaws.com"

/* KVS optional configuration */
#define ENABLE_AUDIO_TRACK              1
#define ENABLE_IOT_CREDENTIAL           0
#define ENABLE_RING_BUFFER_MEM_LIMIT    1
#define DEBUG_STORE_MEDIA_TO_FILE       0
/* Video configuration */
#define VIDEO_TRACK_NAME                "kvs video track"

/* Audio configuration */
#if ENABLE_AUDIO_TRACK
#define USE_AUDIO_AAC                   1   /* Set to 1 to use AAC as audio track */
#define USE_AUDIO_G711                  0   /* Set to 1 to use G711 as audio track */

#if (USE_AUDIO_AAC == 0) && (USE_AUDIO_G711 == 0)
#error "Please select audio source"
#endif

#if USE_AUDIO_AAC
#define AUDIO_CODEC_NAME                "A_AAC"
#define AUDIO_CODEC_OBJECT_TYPE         MPEG4_AAC_LC
#endif /* USE_AUDIO_AAC */

#if USE_AUDIO_G711
#define AUDIO_CODEC_NAME                "A_MS/ACM"
#define AUDIO_CODEC_OBJECT_TYPE         PCM_FORMAT_CODE_ALAW
#endif /* USE_AUDIO_G711 */

#define AUDIO_TRACK_NAME                "kvs audio track"
#define AUDIO_FREQUENCY                 8000
#define AUDIO_CHANNEL_NUMBER            1
#endif /* ENABLE_AUDIO_TRACK */

/* IoT credential configuration */
#if ENABLE_IOT_CREDENTIAL
#define CREDENTIALS_HOST                "xxxxxxxxxxxxxx.credentials.iot.us-east-1.amazonaws.com"
#define ROLE_ALIAS                      "KvsCameraIoTRoleAlias"
#define THING_NAME                      KVS_STREAM_NAME

#define ROOT_CA \
"-----BEGIN CERTIFICATE-----\n" \
"MIIDQTCCAimgAwIBAgITBmyfz5m/jAo54vB4ikPmljZbyjANBgkqhkiG9w0BAQsF
ADA5MQswCQYDVQQGEwJVUzEPMA0GA1UEChMGQW1hem9uMRkwFwYDVQQDExBBbWF6
b24gUm9vdCBDQSAxMB4XDTE1MDUyNjAwMDAwMFoXDTM4MDExNzAwMDAwMFowOTEL
MAkGA1UEBhMCVVMxDzANBgNVBAoTBkFtYXpvbjEZMBcGA1UEAxMQQW1hem9uIFJv
b3QgQ0EgMTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALJ4gHHKeNXj
ca9HgFB0fW7Y14h29Jlo91ghYPl0hAEvrAIthtOgQ3pOsqTQNroBvo3bSMgHFzZM
9O6II8c+6zf1tRn4SWiw3te5djgdYZ6k/oI2peVKVuRF4fn9tBb6dNqcmzU5L/qw
IFAGbHrQgLKm+a/sRxmPUDgH3KKHOVj4utWp+UhnMJbulHheb4mjUcAwhmahRWa6
VOujw5H5SNz/0egwLX0tdHA114gk957EWW67c4cX8jJGKLhD+rcdqsq08p8kDi1L
93FcXmn/6pUCyziKrlA4b9v7LWIbxcceVOF34GfID5yHI9Y/QCB/IIDEgEw+OyQm
jgSubJrIqg0CAwEAAaNCMEAwDwYDVR0TAQH/BAUwAwEB/zAOBgNVHQ8BAf8EBAMC
AYYwHQYDVR0OBBYEFIQYzIU07LwMlJQuCFmcx7IQTgoIMA0GCSqGSIb3DQEBCwUA
A4IBAQCY8jdaQZChGsV2USggNiMOruYou6r4lK5IpDB/G/wkjUu0yKGX9rbxenDI
U5PMCCjjmCXPI6T53iHTfIUJrU6adTrCC2qJeHZERxhlbI1Bjjt/msv0tadQ1wUs
N+gDS63pYaACbvXy8MWy7Vu33PqUXHeeE6V/Uq2V8viTO96LXFvKWlJbYK8U90vv
o/ufQJVtMVT8QtPHRh8jrdkPSHCa2XV4cdFyQzR1bldZwgJcJmApzyMZFo6IQ6XU
5MsI+yMRQ+hDKXJioaldXgjUkK642M4UwtBV8ob2xJNDd2ZhwLnoQdeXeGADbkpy
rqXRfboQnoZsG4q5WTP468SQvvG5\n" \
"-----END CERTIFICATE-----\n"

#define CERTIFICATE \
"-----BEGIN CERTIFICATE-----\n" \
"......\n" \
"-----END CERTIFICATE-----\n"

#define PRIVATE_KEY \
"-----BEGIN RSA PRIVATE KEY-----\n" \
"......\n" \
"-----END RSA PRIVATE KEY-----\n"
#endif /* ENABLE_IOT_CREDENTIAL */

#if ENABLE_RING_BUFFER_MEM_LIMIT
/* Buffering options */
#define RING_BUFFER_MEM_LIMIT           (2 * 1024 * 1024)
#endif /* ENABLE_RING_BUFFER_MEM_LIMIT */

#ifdef KVS_USE_POOL_ALLOCATOR

/**
 * KVS LIB and its 3rd party dependencies use 48K bytes which is measured on RPi. Make it 128K for safety.
 */
#define POOL_ALLOCATOR_SIZE_FOR_KVS     (128 * 1024)

/**
 * Reserve 512K for application usage.
 */
#define POOL_ALLOCATOR_SIZE_FOR_APP     (512 * 1024)

/**
 * Get the size of stream buffer.  If there is no buffer limit, then assume it's 2M bytes.
 */
#if ENABLE_RING_BUFFER_MEM_LIMIT
#define BUFFER_MEM_LIMIT        RING_BUFFER_MEM_LIMIT
#else
#define BUFFER_MEM_LIMIT        (2 * 1024 * 1024)
#endif

/**
 * Total size for pool equals the sum of KVS and its libraries usage, the stream buffer usage, and application usage.
 */
#define POOL_ALLOCATOR_SIZE     (BUFFER_MEM_LIMIT + POOL_ALLOCATOR_SIZE_FOR_KVS + POOL_ALLOCATOR_SIZE_FOR_APP)

#endif /* KVS_USE_POOL_ALLOCATOR */

/* Debug configuration */
#if DEBUG_STORE_MEDIA_TO_FILE
/* Store MKV content in a file with timestamp in its filename. */
#define MEDIA_FILENAME_FORMAT                  "video_%" PRIu64 ".mkv"
#endif /* DEBUG_STORE_MEDIA_TO_FILE */

#endif /* SAMPLE_CONFIG_H */
