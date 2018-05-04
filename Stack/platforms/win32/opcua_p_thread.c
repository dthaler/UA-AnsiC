/* ========================================================================
 * Copyright (c) 2005-2018 The OPC Foundation, Inc. All rights reserved.
 *
 * OPC Foundation MIT License 1.00
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 *
 * The complete license agreement can be found here:
 * http://opcfoundation.org/License/MIT/1.00/
 * ======================================================================*/

/******************************************************************************************************/
/* Platform Portability Layer                                                                         */
/* Modify the content of this file according to the thread implementation on your system.             */
/* This is the pthreads implementation.                                                               */
/* For Information about pthread see http://sourceware.org/pthreads-win32/.                           */
/******************************************************************************************************/

/* #define OPCUA_USE_POSIX 1 */

/* System Headers */
#include <windows.h>
#include <time.h>
#ifdef OPCUA_USE_POSIX
  #include <pthread.h>
#endif /* OPCUA_USE_POSIX */

/* UA platform definitions */
#include <opcua_p_internal.h>

/* additional UA dependencies */
#include <opcua_memory.h>
#include <opcua_p_memory.h>
#include <opcua_semaphore.h>
#include <opcua_mutex.h>

/* own headers */
#include <opcua_thread.h>
#include <opcua_p_thread.h>
#include <opcua_p_openssl.h>

/*============================================================================
 * Port Layer Thread Main
 *===========================================================================*/
typedef struct _OpcUa_P_ThreadArg
{
    OpcUa_Handle                 hThread;
    OpcUa_PfnInternalThreadMain* pfnInternalThreadMain;
    OpcUa_Void*                  ThreadArgs;
} OpcUa_P_ThreadArg;


#ifdef OPCUA_USE_POSIX
/**
* This is the function, the new thread starts with. The only thing to do here,
* is calling the InternalThreadMain from OpcUa_Thread.c and your internal stuff.
*/

OpcUa_Void* pthread_start(OpcUa_Void* args)
{
    OpcUa_P_ThreadArg*  p_P_ThreadArgs      = OpcUa_Null;
    OpcUa_Thread*       pThread             = OpcUa_Null;
    pthread_t*          pInternalThread     = OpcUa_Null;
    int                 apiResult           = 0;


    if(args == OpcUa_Null)
    {
        return OpcUa_Null;
    }

    p_P_ThreadArgs = (OpcUa_P_ThreadArg*)args;


    pInternalThread = (OpcUa_RawThread)(p_P_ThreadArgs->hThread);
    pThread         = p_P_ThreadArgs->ThreadArgs;

    if(pInternalThread == OpcUa_Null)
    {
        return OpcUa_Null;
    }

    /* detach posix thread */
    apiResult = pthread_detach(*pInternalThread);

    if(apiResult != 0)
    {
        return OpcUa_Null;
    }

    /* run stack thread! */
    p_P_ThreadArgs->pfnInternalThreadMain(pThread);

    OpcUa_Free(p_P_ThreadArgs);

    pthread_exit(NULL);

    return OpcUa_Null;
}

/*============================================================================
 * Initialize Raw Thread.
 *===========================================================================*/
OpcUa_Void OpcUa_P_Thread_Initialize(OpcUa_RawThread RawThread)
{
    OpcUa_MemSet((OpcUa_Void*)RawThread, 0, sizeof(pthread_t));
    return;
}

/*============================================================================
 * Create a platform thread
 *===========================================================================*/
OpcUa_StatusCode OpcUa_P_Thread_Create(OpcUa_RawThread* pRawThread)
{
    OpcUa_StatusCode uStatus = OpcUa_Good;
    *pRawThread = (OpcUa_RawThread)OpcUa_Alloc(sizeof(pthread_t));
    OpcUa_ReturnErrorIfAllocFailed(*pRawThread);

    OpcUa_P_Thread_Initialize(*pRawThread);

    return uStatus;
}

/*============================================================================
 * Clear Raw Thread
 *===========================================================================*/
OpcUa_Void OpcUa_P_Thread_Clear(OpcUa_RawThread RawThread)
{
    OpcUa_MemSet((OpcUa_Void*)RawThread, 0, sizeof(pthread_t));
    return;
}

/*============================================================================
 * Delete Raw Thread
 *===========================================================================*/
OpcUa_Void OpcUa_P_Thread_Delete(OpcUa_RawThread* pRawThread)
{
    OpcUa_P_Thread_Clear(*pRawThread);
    OpcUa_Free(*pRawThread);
    *pRawThread = OpcUa_Null;
    return;
}

/*============================================================================
 * Create Thread
 *===========================================================================*/
OpcUa_StatusCode OpcUa_P_Thread_Start(  OpcUa_RawThread             pThread,
                                        OpcUa_PfnInternalThreadMain pfnStartFunction,
                                        OpcUa_Void*                 pArguments)
{
    OpcUa_Int32         apiResult = 0;
    OpcUa_P_ThreadArg*  pThreadArguments = OpcUa_Null;

    if(pThread == OpcUa_Null)
    {
        return OpcUa_BadInvalidArgument;
    }

    pThreadArguments = OpcUa_Alloc(sizeof(OpcUa_P_ThreadArg));
    memset(pThreadArguments, 0, sizeof(OpcUa_P_ThreadArg));

    pThreadArguments->hThread               = pThread;
    pThreadArguments->pfnInternalThreadMain = pfnStartFunction;
    pThreadArguments->ThreadArgs            = pArguments;

    apiResult = pthread_create((pthread_t*)(pThread), NULL, pthread_start, pThreadArguments);


    switch(apiResult)
    {
    case EAGAIN:
        {
            return OpcUa_BadResourceUnavailable;
        }
    case 0:
        {
            return OpcUa_Good;
        }
    default:
        {
            return OpcUa_BadInternalError;
        }
    }
}

/*============================================================================
 * Cancel Thread
 *===========================================================================*/
OpcUa_Void OpcUa_P_Thread_Cancel(OpcUa_RawThread pThread)
{
    /* This may introduce some problems: see Cancellation Points */
    /* No uStatus is returned, we have to wait and see */
    pthread_t* pInternalThread = pThread;

    pthread_cancel(*pInternalThread);
}

/*============================================================================
 * Send the thread to sleep.
 *===========================================================================*/
OpcUa_Void OpcUa_P_Thread_Sleep(OpcUa_UInt32 msecTimeout)
{
    Sleep(msecTimeout);
}

/*============================================================================
 * Get Current Thread Id
 *===========================================================================*/
OpcUa_UInt32 OpcUa_P_Thread_GetCurrentThreadId(OpcUa_Void)
{
    return (OpcUa_UInt32)GetCurrentThreadId();
}

#else /* OPCUA_USE_POSIX */
/**
* This is the function, the new thread starts with. The only thing to do here,
* is calling the InternalThreadMain from OpcUa_Thread.c and your internal stuff.
*/

OpcUa_Void* win32thread_start(OpcUa_Void* args)
{
    OpcUa_P_ThreadArg*  pThreadArgs         = OpcUa_Null;
    OpcUa_Void*         pArguments          = OpcUa_Null;

    if(args == OpcUa_Null)
    {
        return OpcUa_Null;
    }

    pThreadArgs = (OpcUa_P_ThreadArg*)args;

    pArguments  = pThreadArgs->ThreadArgs;

    /* run stack thread! */
    pThreadArgs->pfnInternalThreadMain(pArguments);

#if OPCUA_REQUIRE_OPENSSL
    OpcUa_P_OpenSSL_Thread_Cleanup();
#endif

    ExitThread(0);

    return OpcUa_Null;
}

/*============================================================================
 * Create a platform thread
 *===========================================================================*/
OpcUa_StatusCode OPCUA_DLLCALL OpcUa_P_Thread_Create(OpcUa_RawThread* pRawThread)
{
    OpcUa_StatusCode   uStatus     = OpcUa_Good;
    OpcUa_P_ThreadArg* pThreadArgs = OpcUa_Null;

    *pRawThread = OpcUa_Null;

    pThreadArgs = (OpcUa_P_ThreadArg*)OpcUa_P_Memory_Alloc(sizeof(OpcUa_P_ThreadArg));
    OpcUa_ReturnErrorIfAllocFailed(pThreadArgs);

    pThreadArgs->hThread                = INVALID_HANDLE_VALUE;
    pThreadArgs->pfnInternalThreadMain  = OpcUa_Null;
    pThreadArgs->ThreadArgs             = OpcUa_Null;

    *pRawThread = (OpcUa_RawThread)pThreadArgs;

    return uStatus;
}

/*============================================================================
 * Delete Raw Thread
 *===========================================================================*/
OpcUa_Void OPCUA_DLLCALL OpcUa_P_Thread_Delete(OpcUa_RawThread* pRawThread)
{
    OpcUa_P_ThreadArg* pThreadArgs = OpcUa_Null;

    if(pRawThread == OpcUa_Null || *pRawThread == OpcUa_Null)
    {
        return;
    }
    else
    {
        pThreadArgs = *pRawThread;

        if(INVALID_HANDLE_VALUE != pThreadArgs->hThread)
        {
            WaitForSingleObject(pThreadArgs->hThread, INFINITE);
            CloseHandle(pThreadArgs->hThread);
        }

        pThreadArgs->hThread = INVALID_HANDLE_VALUE;
        pThreadArgs->pfnInternalThreadMain = OpcUa_Null;
        pThreadArgs->ThreadArgs = OpcUa_Null;
    }
    OpcUa_P_Memory_Free(pThreadArgs);
    *pRawThread = OpcUa_Null;
    return;
}

/*============================================================================
 * Create Thread
 *===========================================================================*/
OpcUa_StatusCode OPCUA_DLLCALL OpcUa_P_Thread_Start(OpcUa_RawThread             pThread,
                                                    OpcUa_PfnInternalThreadMain pfnStartFunction,
                                                    OpcUa_Void*                 pArguments)
{
    HANDLE threadHandle = 0;

    if(pThread == OpcUa_Null)
    {
        return OpcUa_BadInvalidArgument;
    }

    ((OpcUa_P_ThreadArg*)pThread)->pfnInternalThreadMain    = pfnStartFunction;
    ((OpcUa_P_ThreadArg*)pThread)->ThreadArgs               = pArguments;

    threadHandle = CreateThread(    NULL,
                                    0,
                                    (LPTHREAD_START_ROUTINE)win32thread_start,
                                    pThread,
                                    0,
                                    NULL);

    if(threadHandle == NULL)
    {
        return OpcUa_BadResourceUnavailable;
    }
    else
    {
        ((OpcUa_P_ThreadArg*)pThread)->hThread = threadHandle;
        return OpcUa_Good;
    }
}

/*============================================================================
 * Send the thread to sleep.
 *===========================================================================*/
OpcUa_Void OPCUA_DLLCALL OpcUa_P_Thread_Sleep(OpcUa_UInt32 msecTimeout)
{
    Sleep(msecTimeout);
}

/*============================================================================
 * Get Current Thread Id
 *===========================================================================*/
OpcUa_UInt32 OPCUA_DLLCALL OpcUa_P_Thread_GetCurrentThreadId(OpcUa_Void)
{
    return (OpcUa_UInt32)GetCurrentThreadId();
}

#endif /* OPCUA_USE_POSIX */
