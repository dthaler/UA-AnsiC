/*  ========================================================================
 * Copyright (c) 2005-2016 The OPC Foundation, Inc. All rights reserved.
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
 
/* serverstub (basic includes for implementing a server based on the stack) */
#include <opcua_serverstub.h>
#include <opcua_string.h>
#include <opcua_memory.h>
#include <opcua_core.h>

#include "addressspace.h"
#include "general_header.h"
#include "subscriptionservice.h"
#include "mytrace.h"
#include "browseservice.h"
#include "readservice.h"

/* The minimum interval between change notifications we will send, in milliseconds.
 * This is also the sampling interval.
 */
#define PUBLISHING_INTERVAL_MS 1000

/* The number of publishing intervals with no changes after which to send a keepalive. */
#define MAX_KEEP_ALIVE_COUNT      5

/* The number of publishing intervals after which to delete a subscription if no PUBLISH is received. 
 * Per spec, this must be at least 3 times the keep alive count.
 */
#define LIFETIME_COUNT        (MAX_KEEP_ALIVE_COUNT * 3)

/* We currently use one global timer rather than a separate timer per subscription as
 * the spec describes, since we only support the same constant interval for all subscriptions.
 */
OpcUa_Timer UaTestServer_g_SubscriptionTimer = OpcUa_Null;

/* Mutex protecting the list links. */
OpcUa_Mutex UaTestServer_g_hSubscriptionMutex = OpcUa_Null;

typedef struct _MyMonitoredItem
{
    struct _MyMonitoredItem* Prev;
    struct _MyMonitoredItem* Next;
    OpcUa_UInt32             AttributeId;
    OpcUa_NodeId             NodeId;
    OpcUa_UInt32             Id;
    OpcUa_UInt32             ClientHandle;
    OpcUa_Boolean            Dirty; /* True if needs a notification sent. */
    OpcUa_TimestampsToReturn TimestampsToReturn;
    OpcUa_DataValue          LastValue;
} MyMonitoredItem;

typedef struct _MySubscription
{
    struct _MySubscription* Prev;
    struct _MySubscription* Next;
    SessionData*            pSession;
    OpcUa_UInt32            Id;
    OpcUa_UInt32            SeqNum;
    OpcUa_UInt32            LastSequenceNumberAcknowledged;
    OpcUa_Endpoint          hEndpoint;
    OpcUa_Handle            hContext;
    OpcUa_UInt32            NotificationsAvailable; /* Number of monitored items that need to have notifications. */
    OpcUa_Boolean           LatePublishRequest;
    OpcUa_Boolean           PublishingEnabled;

    /* The number of consecutive publishing intervals left before cleaning up state
     * unless we have a notification or keepalive exchange.
     */
    OpcUa_UInt32            LifetimeCounter;

    MyMonitoredItem         MonitoredItemsHead;
} MySubscription;

typedef struct _MyPublishQueueItem
{
    struct _MyPublishQueueItem* Prev;
    struct _MyPublishQueueItem* Next;
    OpcUa_Endpoint              hEndpoint;
    OpcUa_Handle                hContext;
    OpcUa_PublishRequest*       pRequest;
    OpcUa_EncodeableType*       pRequestType;
    OpcUa_PublishResponse*      pResponse;
    OpcUa_EncodeableType*       pResponseType;
} MyPublishQueueItem;

/* List of queued publish requests available for us to complete with change notifications. */
MyPublishQueueItem UaTestServer_g_PublishQueueItemsHead = { 
    &UaTestServer_g_PublishQueueItemsHead, 
    &UaTestServer_g_PublishQueueItemsHead,
    OpcUa_Null,
    OpcUa_Null,
    OpcUa_Null,
};

MySubscription UaTestServer_g_SubscriptionsHead = { 
    &UaTestServer_g_SubscriptionsHead, 
    &UaTestServer_g_SubscriptionsHead,
    0   /* Last subscription ID used.  The value 0 is reserved by spec to mean none. */
};

/*============================================================================
* Create state for a publish request queue item, but don't queue it yet.
*===========================================================================*/
MyPublishQueueItem* create_publish_queue_item(
    OpcUa_Endpoint         a_hEndpoint,
    OpcUa_Handle           a_hContext,
    OpcUa_PublishRequest** a_ppRequest,
    OpcUa_EncodeableType*  a_pRequestType)
{
    OpcUa_StatusCode uStatus;
    MyPublishQueueItem* pPublishQueueItem = OpcUa_Alloc(sizeof(*pPublishQueueItem));
    if (pPublishQueueItem == OpcUa_Null)
    {
        return OpcUa_Null;
    }
    OpcUa_MemSet(pPublishQueueItem, 0, sizeof(*pPublishQueueItem));

    /* Create a context to use for sending a response. */
    uStatus = OpcUa_Endpoint_BeginSendResponse(
        a_hEndpoint,
        a_hContext,
        (OpcUa_Void**)&pPublishQueueItem->pResponse,
        &pPublishQueueItem->pResponseType);
    if (OpcUa_IsBad(uStatus))
    {
        OpcUa_Free(pPublishQueueItem);
        return OpcUa_Null;
    }
    pPublishQueueItem->pResponse->NoOfDiagnosticInfos = 0;
    pPublishQueueItem->pResponse->DiagnosticInfos = OpcUa_Null;
    OpcUa_NotificationMessage_Initialize(&pPublishQueueItem->pResponse->NotificationMessage);
    pPublishQueueItem->hEndpoint = a_hEndpoint;
    pPublishQueueItem->hContext = a_hContext;
    pPublishQueueItem->pRequestType = a_pRequestType;

    /* Take ownership of the request buffer. */
    pPublishQueueItem->pRequest = *a_ppRequest;
    *a_ppRequest = OpcUa_Null;

    return pPublishQueueItem;
}

/*============================================================================
* Queue a publish request until we have something to sent, which we check at each
* publish interval.  This is the EnqueuePublishingReq() function in the OPC-UA spec.
*===========================================================================*/
OpcUa_StatusCode my_EnqueuePublishingReq(MyPublishQueueItem* a_pPublishQueueItem)
{
    OpcUa_InitializeStatus(OpcUa_Module_Server, "add_publish_request");

    a_pPublishQueueItem->Next = &UaTestServer_g_PublishQueueItemsHead;
    a_pPublishQueueItem->Prev = UaTestServer_g_PublishQueueItemsHead.Prev;
    a_pPublishQueueItem->Next->Prev = a_pPublishQueueItem;
    a_pPublishQueueItem->Prev->Next = a_pPublishQueueItem;

    OpcUa_ReturnStatusCode;
    OpcUa_BeginErrorHandling;
    OpcUa_FinishErrorHandling;
}

/*============================================================================
* Create state for a new monitored item in a given subscription.
*===========================================================================*/
OpcUa_StatusCode add_monitored_item(
    MySubscription*          a_pSubscription, 
    OpcUa_UInt32             a_AttributeId,
    OpcUa_NodeId             a_NodeId,
    OpcUa_UInt32             a_ClientHandle,
    OpcUa_TimestampsToReturn a_TimestampsToReturn,
    MyMonitoredItem**        a_ppMonitoredItem)
{
    MyMonitoredItem* pMonitoredItem;

    OpcUa_InitializeStatus(OpcUa_Module_Server, "add_monitored_item");

    pMonitoredItem = OpcUa_Alloc(sizeof(*pMonitoredItem));
    OpcUa_GotoErrorIfAllocFailed(pMonitoredItem);

    uStatus = OpcUa_NodeId_CopyTo(&a_NodeId, &pMonitoredItem->NodeId);
    OpcUa_GotoErrorIfBad(uStatus);

    pMonitoredItem->Next = &a_pSubscription->MonitoredItemsHead;
    pMonitoredItem->Prev = a_pSubscription->MonitoredItemsHead.Prev;
    pMonitoredItem->Next->Prev = pMonitoredItem;
    pMonitoredItem->Prev->Next = pMonitoredItem;
    pMonitoredItem->ClientHandle = a_ClientHandle;
    pMonitoredItem->TimestampsToReturn = a_TimestampsToReturn;
    pMonitoredItem->Id = ++a_pSubscription->MonitoredItemsHead.Id;
    pMonitoredItem->AttributeId = a_AttributeId;
    if (a_AttributeId == OpcUa_Attributes_Value)
    {
        pMonitoredItem->Dirty = OpcUa_True;
        a_pSubscription->NotificationsAvailable++;
    }
    OpcUa_DataValue_Initialize(&pMonitoredItem->LastValue);

    *a_ppMonitoredItem = pMonitoredItem;

    OpcUa_ReturnStatusCode;
    OpcUa_BeginErrorHandling;
    OpcUa_FinishErrorHandling;
}

/*============================================================================
* Remove a monitored item from its subscription, and free it.
*===========================================================================*/
OpcUa_Void delete_monitored_item(MyMonitoredItem* a_pMonitoredItem)
{
    a_pMonitoredItem->Next->Prev = a_pMonitoredItem->Prev;
    a_pMonitoredItem->Prev->Next = a_pMonitoredItem->Next;
    OpcUa_NodeId_Clear(&a_pMonitoredItem->NodeId);
    
    OpcUa_Free(a_pMonitoredItem);
}

/*============================================================================
* Delete state for a given subscription.
*===========================================================================*/
OpcUa_Void delete_subscription(MySubscription* a_pSubscription)
{
    /* Remove from global list. */
    a_pSubscription->Next->Prev = a_pSubscription->Prev;
    a_pSubscription->Prev->Next = a_pSubscription->Next;
    a_pSubscription->Next = OpcUa_Null;
    a_pSubscription->Prev = OpcUa_Null;

    /* Delete all monitored items. */
    while (a_pSubscription->MonitoredItemsHead.Next != &a_pSubscription->MonitoredItemsHead)
    {
        delete_monitored_item(a_pSubscription->MonitoredItemsHead.Next);
    }

    /* Stop timer if there are no subscriptions left. */
    if (UaTestServer_g_SubscriptionsHead.Next == &UaTestServer_g_SubscriptionsHead)
    {
        OpcUa_Timer_Delete(&UaTestServer_g_SubscriptionTimer);
    }

    OpcUa_Free(a_pSubscription);
}

OpcUa_Void delete_all_subscriptions(SessionData* a_pSession)
{
    MySubscription* pSubscription = OpcUa_Null;

    OpcUa_Mutex_Lock(UaTestServer_g_hSubscriptionMutex);

    for (pSubscription = UaTestServer_g_SubscriptionsHead.Next;
         pSubscription != &UaTestServer_g_SubscriptionsHead;
         pSubscription = pSubscription->Next)
    {
        if (pSubscription->pSession == a_pSession)
        {
            pSubscription = pSubscription->Prev;
            delete_subscription(pSubscription->Next);
            continue;
        }
    }

    OpcUa_Mutex_Unlock(UaTestServer_g_hSubscriptionMutex);
}

/*============================================================================
* Reset the lifetime counter on a subscription so it won't expire.
* This is the ResetLifetimeCounter() function in the OPC-UA spec.
*===========================================================================*/
OpcUa_Void my_ResetLifetimeCounter(MySubscription* a_pSubscription)
{
    a_pSubscription->LifetimeCounter = LIFETIME_COUNT;
}

OpcUa_Boolean is_value_different(OpcUa_DataValue* a, OpcUa_DataValue* b)
{
    if ((a->Value.Datatype != b->Value.Datatype) ||
        (a->Value.ArrayType != b->Value.ArrayType))
    {
        return OpcUa_True;
    }

    if (a->Value.ArrayType != OpcUa_VariantArrayType_Scalar)
    {
        /* We don't currently support arrays. */
        return OpcUa_False;
    }

    switch (a->Value.Datatype)
    {
    case OpcUaId_Double: 
        return (a->Value.Value.Double != b->Value.Value.Double);
    case OpcUaId_DateTime: 
        return (a->Value.Value.DateTime.dwHighDateTime != b->Value.Value.DateTime.dwHighDateTime) ||
               (a->Value.Value.DateTime.dwLowDateTime != b->Value.Value.DateTime.dwLowDateTime);
    case OpcUaId_String:
        {
            OpcUa_Int32 diff;
            OpcUa_StatusCode uStatus = OpcUa_String_Compare(&a->Value.Value.String, &b->Value.Value.String, &diff);
            return OpcUa_IsGood(uStatus) && (diff != 0);
        }
    case OpcUaId_UInt32:
        return (a->Value.Value.UInt32 != b->Value.Value.UInt32);
    case OpcUaId_Int16:
        return (a->Value.Value.Int16 != b->Value.Value.Int16);
    case OpcUaId_Boolean:
        return (a->Value.Value.Boolean != b->Value.Value.Boolean);
    default: 
        /* We don't currently support other types. */
        return OpcUa_False;
    }
}

OpcUa_StatusCode copy_data_value(OpcUa_DataValue* a_pDestValue, OpcUa_DataValue* a_pSourceValue)
{
    OpcUa_DataValue_Clear(a_pDestValue);

    if (a_pSourceValue->Value.ArrayType != OpcUa_VariantArrayType_Scalar)
    {
        return OpcUa_BadNotImplemented;
    }

    /* Copy fields other than the actual value. */
    OpcUa_MemCpy(a_pDestValue, sizeof(*a_pDestValue),
                 a_pSourceValue, sizeof(*a_pSourceValue));
    OpcUa_MemSet(&a_pDestValue->Value.Value, 0, sizeof(a_pDestValue->Value.Value));

    switch (a_pSourceValue->Value.Datatype)
    {
    case OpcUaId_Double:
        a_pDestValue->Value.Value.Double = a_pSourceValue->Value.Value.Double;
        break;
    case OpcUaId_DateTime:
        a_pDestValue->Value.Value.DateTime = a_pSourceValue->Value.Value.DateTime;
        break;
    case OpcUaId_String:
        OpcUa_String_CopyTo(&a_pSourceValue->Value.Value.String, &a_pDestValue->Value.Value.String);
        break;
    case OpcUaId_UInt32:
        a_pDestValue->Value.Value.UInt32 = a_pSourceValue->Value.Value.UInt32;
        break;
    case OpcUaId_Int16:
        a_pDestValue->Value.Value.Int16 = a_pSourceValue->Value.Value.Int16;
        break;
    case OpcUaId_Boolean:
        a_pDestValue->Value.Value.Boolean = a_pSourceValue->Value.Value.Boolean;
        break;
    default:
        return OpcUa_BadNotImplemented;
    }

    return OpcUa_Good;
}

OpcUa_Boolean needs_notification(MySubscription* a_pSubscription, OpcUa_Boolean a_bTick)
{
    MyMonitoredItem* pMonitoredItem;

    a_pSubscription->NotificationsAvailable = 0;

    if (!a_pSubscription->PublishingEnabled)
    {
        return OpcUa_False;
    }

    /* Check whether the values of any monitored items have changed since the last notification. */
    for (pMonitoredItem = a_pSubscription->MonitoredItemsHead.Next;
        pMonitoredItem != &a_pSubscription->MonitoredItemsHead;
        pMonitoredItem = pMonitoredItem->Next)
    {
        if (pMonitoredItem->AttributeId != OpcUa_Attributes_Value)
        {
            continue;
        }

        OpcUa_DataValue currentValue;
        OpcUa_DataValue_Initialize(&currentValue);

        _VariableNode_* p_Node = (_VariableNode_*)search_for_node(pMonitoredItem->NodeId);
        if (p_Node == OpcUa_Null)
        {
            continue;
        }
        if (p_Node->BaseAttribute.NodeClass != OpcUa_NodeClass_Variable)
        {
            continue;
        }

        /* Check whether the value actually changed. We have the client's TimestampsToReturn
         * value in our pMonitoreditem state, but fill_data_value currently won't allow
         * monitoring the CurrentTime variable with UAExpert which always sets it to Neither.
         * So as a workaround, we always pass OpcUa_TimestampsToReturn_Server;
         */
        const OpcUa_TimestampsToReturn ts = OpcUa_TimestampsToReturn_Server;
        if (OpcUa_IsBad(fill_data_value(&currentValue,
                                        p_Node,
                                        ts)))
        {
            continue;
        }

        if (!pMonitoredItem->Dirty && !is_value_different(&pMonitoredItem->LastValue, &currentValue))
        {
            continue;
        }

        if (currentValue.Value.Datatype == 0)
        {
            /* No value exists. */
            pMonitoredItem->Dirty = OpcUa_False;
            continue;
        }

        pMonitoredItem->Dirty = OpcUa_True;
        a_pSubscription->NotificationsAvailable++;

        copy_data_value(&pMonitoredItem->LastValue, &currentValue);
        OpcUa_DataValue_Clear(&currentValue);
    }

    if (a_pSubscription->NotificationsAvailable == 0)
    {
        if (a_bTick)
        {
            a_pSubscription->LifetimeCounter--;
        }
    }
    else
    {
        return OpcUa_True;
    }

    /* The spec requires us to respond with a keepalive if we have sent no messages so
    * far for a subscription.
    */
    if (a_pSubscription->SeqNum == 0)
    {
        return OpcUa_True;
    }

    /* Check whether this subscription is now expired. */
    if (a_pSubscription->LifetimeCounter == 0)
    {
        /* We don't currently support event notifications. */

#ifndef NO_DEBUGGING_
        MY_TRACE("\n\n\n=====SUBSCRIPTION %d EXPIRED ON SESSION %d====================================\n",
            a_pSubscription->Id, a_pSubscription->pSession->SessionId.Identifier.Numeric);
#endif /*_DEBUGGING_*/

        /* Clean up state. */
        a_pSubscription = a_pSubscription->Prev;
        delete_subscription(a_pSubscription->Next);
        return OpcUa_False;
    }

    /* Check whether we need to send a keepalive. */
    if ((a_pSubscription->LifetimeCounter % MAX_KEEP_ALIVE_COUNT) == 0)
    {
        return OpcUa_True;
    }

    return OpcUa_False;
}

/*============================================================================
 * Get the first subscription that we need to send notifications for, or null if none.
 *===========================================================================*/
MySubscription* get_changed_subscription(SessionData* a_pSession, OpcUa_Boolean tick)
{
    MySubscription* pSubscription;
    for (pSubscription = UaTestServer_g_SubscriptionsHead.Next;
        pSubscription != &UaTestServer_g_SubscriptionsHead;
        pSubscription = pSubscription->Next)
    {
        if ((pSubscription->pSession == a_pSession) && needs_notification(pSubscription, OpcUa_False))
        {
            return pSubscription;
        }
    }
    return OpcUa_Null;
}

/*============================================================================
* Free a publish queue item.
*===========================================================================*/
OpcUa_Void free_publish_queue_item(MyPublishQueueItem* a_pPublishQueueItem)
{
    if (a_pPublishQueueItem->pRequest != OpcUa_Null)
    {
        OpcUa_EncodeableObject_Delete(a_pPublishQueueItem->pRequestType, &a_pPublishQueueItem->pRequest);
    }
    if (a_pPublishQueueItem->pResponse != OpcUa_Null)
    {
        OpcUa_EncodeableObject_Delete(a_pPublishQueueItem->pResponseType, &a_pPublishQueueItem->pResponse);
    }

    OpcUa_Free(a_pPublishQueueItem);
}

/*============================================================================
* Remove an item from the publish request queue.
* This is the DequeuePublishReq() function in the OPC-UA spec.
*===========================================================================*/
MyPublishQueueItem* my_DequeuePublishReq(SessionData* a_pSession)
{
    MyPublishQueueItem* pPublishQueueItem;
    OpcUa_UInt32 sessionId = a_pSession->SessionId.Identifier.Numeric;

    for (pPublishQueueItem = UaTestServer_g_PublishQueueItemsHead.Next;
        pPublishQueueItem != &UaTestServer_g_PublishQueueItemsHead;
        pPublishQueueItem = pPublishQueueItem->Next)
    {
        if (pPublishQueueItem->pRequest->RequestHeader.AuthenticationToken.Identifier.Numeric == sessionId)
        {
            break;
        }
    }
    if (pPublishQueueItem == &UaTestServer_g_PublishQueueItemsHead)
    {
        pPublishQueueItem = OpcUa_Null;
    }
    else
    {
        pPublishQueueItem->Next->Prev = pPublishQueueItem->Prev;
        pPublishQueueItem->Prev->Next = pPublishQueueItem->Next;
    }

    return pPublishQueueItem;
}

OpcUa_StatusCode my_CompletePublish(
    MyPublishQueueItem* a_pPublishQueueItem,
    MySubscription*     a_pSubscription,
    OpcUa_StatusCode    a_uStatus);

OpcUa_StatusCode OPCUA_DLLCALL PublishTimer_Callback(
    OpcUa_Void*   a_pvCallbackData,
    OpcUa_Timer   a_hTimer,
    OpcUa_UInt32  a_msecElapsed)
{
    MySubscription* pSubscription = OpcUa_Null;

    OpcUa_InitializeStatus(OpcUa_Module_Server, "add_subscription");

    OpcUa_Mutex_Lock(UaTestServer_g_hSubscriptionMutex);

    for (pSubscription = UaTestServer_g_SubscriptionsHead.Next;
        pSubscription != &UaTestServer_g_SubscriptionsHead;
        pSubscription = pSubscription->Next)
    {
        if (!needs_notification(pSubscription, OpcUa_True))
        {
            /* Nothing to do for this subscription. */
            continue;
        }

        /* We have something to send, either a keepalive or a change notification. */

        my_ResetLifetimeCounter(pSubscription);

        MyPublishQueueItem* pPublishQueueItem = my_DequeuePublishReq(pSubscription->pSession);
        if (pPublishQueueItem == OpcUa_Null)
        {
#ifndef NO_DEBUGGING_
            MY_TRACE("\n\n\n=====NO PUBLISH REQUEST QUEUED FOR SUBSCRIPTION %d ON SESSION %d (SKIPPING)====================================\n",
                pSubscription->Id, pSubscription->pSession->SessionId.Identifier.Numeric);
#endif /*_DEBUGGING_*/

            pSubscription->LatePublishRequest = OpcUa_True;
        }
        else
        {
            /* Return Good to the client, so the timer will still continue running. */
            (void)my_CompletePublish(pPublishQueueItem, pSubscription, OpcUa_Good);
        }
    }

    OpcUa_Mutex_Unlock(UaTestServer_g_hSubscriptionMutex);

    OpcUa_ReturnStatusCode;
    OpcUa_BeginErrorHandling;

    OpcUa_Mutex_Unlock(UaTestServer_g_hSubscriptionMutex);

    OpcUa_FinishErrorHandling;

    return OpcUa_Good;
}

/*============================================================================
* Create state for a new subscription.
*===========================================================================*/
OpcUa_StatusCode add_subscription(
    SessionData*     a_pSession,
    OpcUa_Endpoint   a_hEndpoint,
    OpcUa_Handle     a_hContext,
    MySubscription** a_ppSubscription)
{
    MySubscription* pSubscription;

    OpcUa_InitializeStatus(OpcUa_Module_Server, "add_subscription");

    pSubscription = OpcUa_Alloc(sizeof(*pSubscription));
    OpcUa_GotoErrorIfAllocFailed(pSubscription);
    OpcUa_MemSet(pSubscription, 0, sizeof(*pSubscription));
    pSubscription->pSession = a_pSession;

    pSubscription->Next = &UaTestServer_g_SubscriptionsHead;
    pSubscription->Prev = UaTestServer_g_SubscriptionsHead.Prev;
    pSubscription->Next->Prev = pSubscription;
    pSubscription->Prev->Next = pSubscription;
    pSubscription->Id = ++UaTestServer_g_SubscriptionsHead.Id;

    /* Start publish timer, if not already running. */
    if (UaTestServer_g_SubscriptionTimer == OpcUa_Null)
    {
        if (OpcUa_IsBad(OpcUa_Timer_Create(
            &UaTestServer_g_SubscriptionTimer,
            PUBLISHING_INTERVAL_MS,
            &PublishTimer_Callback,
            OpcUa_Null,
            OpcUa_Null)))
        {
            uStatus = OpcUa_BadInternalError;
        }
    }
    
    OpcUa_GotoErrorIfBad(uStatus);

    my_ResetLifetimeCounter(pSubscription);
    pSubscription->MonitoredItemsHead.Next = &pSubscription->MonitoredItemsHead;
    pSubscription->MonitoredItemsHead.Prev = &pSubscription->MonitoredItemsHead;
    pSubscription->MonitoredItemsHead.Id = 0; /* Last monitored item ID used. */
    pSubscription->hEndpoint = a_hEndpoint;
    pSubscription->hContext = a_hContext;

    *a_ppSubscription = pSubscription;

    OpcUa_ReturnStatusCode;
    OpcUa_BeginErrorHandling;
    OpcUa_FinishErrorHandling;
}

/*============================================================================
* Search for a subscription by its id.
*===========================================================================*/
MySubscription* find_subscription(OpcUa_UInt32 a_uId)
{
    MySubscription* pSubscription = OpcUa_Null;

    for (pSubscription = UaTestServer_g_SubscriptionsHead.Next;
         pSubscription != &UaTestServer_g_SubscriptionsHead;
         pSubscription = pSubscription->Next)
    {
        if (pSubscription->Id == a_uId)
        {
            break;
        }
    }

    return (pSubscription == &UaTestServer_g_SubscriptionsHead) ? OpcUa_Null : pSubscription;
}

/*============================================================================
* Search for a monitored item by its id in a given subscription.
*===========================================================================*/
MyMonitoredItem* find_monitored_item(MySubscription* a_pSubscription, OpcUa_UInt32 a_uId)
{
    MyMonitoredItem* pMonitoredItem = OpcUa_Null;

    for (pMonitoredItem = a_pSubscription->MonitoredItemsHead.Next;
         pMonitoredItem != &a_pSubscription->MonitoredItemsHead;
         pMonitoredItem = pMonitoredItem->Next)
    {
        if (pMonitoredItem->Id == a_uId)
        {
            break;
        }
    }

    return (pMonitoredItem == &a_pSubscription->MonitoredItemsHead) ? OpcUa_Null : pMonitoredItem;
}

/*============================================================================
 * A method that implements the CreateSubscription service.
 *===========================================================================*/
OpcUa_StatusCode my_CreateSubscription(
    OpcUa_Endpoint             a_hEndpoint,
    OpcUa_Handle               a_hContext,
    const OpcUa_RequestHeader* a_pRequestHeader,
    OpcUa_Double               a_nRequestedPublishingInterval,
    OpcUa_UInt32               a_nRequestedLifetimeCount,
    OpcUa_UInt32               a_nRequestedMaxKeepAliveCount,
    OpcUa_UInt32               a_nMaxNotificationsPerPublish,
    OpcUa_Boolean              a_bPublishingEnabled,
    OpcUa_Byte                 a_nPriority,
    OpcUa_ResponseHeader*      a_pResponseHeader,
    OpcUa_UInt32*              a_pSubscriptionId,
    OpcUa_Double*              a_pRevisedPublishingInterval,
    OpcUa_UInt32*              a_pRevisedLifetimeCount,
    OpcUa_UInt32*              a_pRevisedMaxKeepAliveCount)
{
    MySubscription* pSubscription;
    SessionData* pSession = OpcUa_Null;

    OpcUa_InitializeStatus(OpcUa_Module_Server, "OpcUa_ServerApi_CreateSubscription");

    /* Validate arguments. */
    OpcUa_ReturnErrorIfArgumentNull(a_hEndpoint);
    OpcUa_ReturnErrorIfArgumentNull(a_hContext);
    OpcUa_ReturnErrorIfArgumentNull(a_pRequestHeader);
    OpcUa_ReturnErrorIfArgumentNull(a_pResponseHeader);
    OpcUa_ReturnErrorIfArgumentNull(a_pSubscriptionId);
    OpcUa_ReturnErrorIfArgumentNull(a_pRevisedPublishingInterval);
    OpcUa_ReturnErrorIfArgumentNull(a_pRevisedLifetimeCount);
    OpcUa_ReturnErrorIfArgumentNull(a_pRevisedMaxKeepAliveCount);

    OpcUa_Mutex_Lock(UaTestServer_g_hSubscriptionMutex);

#ifndef NO_DEBUGGING_
    MY_TRACE("\n\n\nCREATESUBSCRIPTION SERVICE ON SESSION %d==============================================\n",
        a_pRequestHeader->AuthenticationToken.Identifier.Numeric);
#endif /*_DEBUGGING_*/

    pSession = UaTestServer_Session_Find(&a_pRequestHeader->AuthenticationToken);
    OpcUa_GotoErrorIfNull(pSession, OpcUa_BadSecurityChecksFailed);

    RESET_SESSION_COUNTER(pSession);

    uStatus = add_subscription(pSession, a_hEndpoint, a_hContext, &pSubscription);
    OpcUa_GotoErrorIfBad(uStatus);
    pSubscription->PublishingEnabled = a_bPublishingEnabled;

#ifndef NO_DEBUGGING_
    MY_TRACE("PublishingEnabled: %d\n", pSubscription->PublishingEnabled);
    MY_TRACE("Id: %d\n", pSubscription->Id);
#endif /*_DEBUGGING_*/

    *a_pSubscriptionId = pSubscription->Id;
    *a_pRevisedPublishingInterval = PUBLISHING_INTERVAL_MS;
    *a_pRevisedLifetimeCount = LIFETIME_COUNT;
    *a_pRevisedMaxKeepAliveCount = MAX_KEEP_ALIVE_COUNT;

    uStatus = response_header_fill(pSession, a_pResponseHeader, a_pRequestHeader, uStatus);
    if (OpcUa_IsBad(uStatus))
    {
        a_pResponseHeader->ServiceResult = OpcUa_BadInternalError;
    }
#ifndef NO_DEBUGGING_
    MY_TRACE("\nSERVICE===END============================================\n\n\n");
#endif /*_DEBUGGING_*/

    OpcUa_Mutex_Unlock(UaTestServer_g_hSubscriptionMutex);

    OpcUa_ReturnStatusCode;
    OpcUa_BeginErrorHandling;

    uStatus = response_header_fill(pSession, a_pResponseHeader, a_pRequestHeader, uStatus);
    if (OpcUa_IsBad(uStatus))
    {
        a_pResponseHeader->ServiceResult = OpcUa_BadInternalError;
    }
#ifndef NO_DEBUGGING_
        MY_TRACE("\nSERVICE END (WITH ERROR)===========\n\n\n");
#endif /*_DEBUGGING_*/

    OpcUa_Mutex_Unlock(UaTestServer_g_hSubscriptionMutex);

    OpcUa_FinishErrorHandling;
}

/*============================================================================
 * A method that implements the DeleteSubscriptions service.
 *===========================================================================*/
OpcUa_StatusCode my_DeleteSubscriptions(
    OpcUa_Endpoint             a_hEndpoint,
    OpcUa_Handle               a_hContext,
    const OpcUa_RequestHeader* a_pRequestHeader,
    OpcUa_Int32                a_nNoOfSubscriptionIds,
    const OpcUa_UInt32*        a_pSubscriptionIds,
    OpcUa_ResponseHeader*      a_pResponseHeader,
    OpcUa_Int32*               a_pNoOfResults,
    OpcUa_StatusCode**         a_pResults,
    OpcUa_Int32*               a_pNoOfDiagnosticInfos,
    OpcUa_DiagnosticInfo**     a_pDiagnosticInfos)
{
    OpcUa_Int32 i;
    SessionData* pSession = OpcUa_Null;

    OpcUa_InitializeStatus(OpcUa_Module_Server, "OpcUa_ServerApi_DeleteSubscriptions");

    /* Validate arguments. */
    OpcUa_ReturnErrorIfArgumentNull(a_hEndpoint);
    OpcUa_ReturnErrorIfArgumentNull(a_hContext);
    OpcUa_ReturnErrorIfArgumentNull(a_pRequestHeader);
    OpcUa_ReturnErrorIfArrayArgumentNull(a_nNoOfSubscriptionIds, a_pSubscriptionIds);
    OpcUa_ReturnErrorIfArgumentNull(a_pResponseHeader);
    OpcUa_ReturnErrorIfArrayArgumentNull(a_pNoOfResults, a_pResults);
    OpcUa_ReturnErrorIfArrayArgumentNull(a_pNoOfDiagnosticInfos, a_pDiagnosticInfos);

    *a_pNoOfDiagnosticInfos = 0;
    *a_pDiagnosticInfos = OpcUa_Null;

#ifndef NO_DEBUGGING_
    MY_TRACE("\n\n\nDELETESUBSCRIPTIONS SERVICE ON SESSION %d==============================================\n",
        a_pRequestHeader->AuthenticationToken.Identifier.Numeric);
#endif /*_DEBUGGING_*/

    OpcUa_Mutex_Lock(UaTestServer_g_hSubscriptionMutex);

    pSession = UaTestServer_Session_Find(&a_pRequestHeader->AuthenticationToken);
    OpcUa_GotoErrorIfNull(pSession, OpcUa_BadSecurityChecksFailed);

    RESET_SESSION_COUNTER(pSession);

    *a_pResults = OpcUa_Alloc(a_nNoOfSubscriptionIds * sizeof(OpcUa_StatusCode));
    OpcUa_GotoErrorIfAllocFailed(*a_pResults);

    for (i = 0; i < a_nNoOfSubscriptionIds; i++)
    {
        OpcUa_UInt32 id = a_pSubscriptionIds[i];

        MySubscription* pSubscription = find_subscription(id);
        if (pSubscription == OpcUa_Null)
        {
            (*a_pResults)[i] = OpcUa_BadSubscriptionIdInvalid;
        }
        else
        {
            delete_subscription(pSubscription);
            (*a_pResults)[i] = OpcUa_Good;
        }
    }
    
    *a_pNoOfResults = a_nNoOfSubscriptionIds;

    uStatus = response_header_fill(pSession, a_pResponseHeader, a_pRequestHeader, uStatus);
    if (OpcUa_IsBad(uStatus))
    {
        a_pResponseHeader->ServiceResult = OpcUa_BadInternalError;
    }
#ifndef NO_DEBUGGING_
    MY_TRACE("\nSERVICE===END============================================\n\n\n");
#endif /*_DEBUGGING_*/

    OpcUa_Mutex_Unlock(UaTestServer_g_hSubscriptionMutex);

    OpcUa_ReturnStatusCode;
    OpcUa_BeginErrorHandling;

    uStatus = response_header_fill(pSession, a_pResponseHeader, a_pRequestHeader, uStatus);
    if (OpcUa_IsBad(uStatus))
    {
        a_pResponseHeader->ServiceResult = OpcUa_BadInternalError;
    }
#ifndef NO_DEBUGGING_
    MY_TRACE("\nSERVICE END (WITH ERROR)===========\n\n\n");
#endif /*_DEBUGGING_*/
    OpcUa_Mutex_Unlock(UaTestServer_g_hSubscriptionMutex);

    OpcUa_FinishErrorHandling;
}

/*============================================================================
 * A method that implements the CreateMonitoredItems service.
 *===========================================================================*/
OpcUa_StatusCode my_CreateMonitoredItems(
    OpcUa_Endpoint                          a_hEndpoint,
    OpcUa_Handle                            a_hContext,
    const OpcUa_RequestHeader*              a_pRequestHeader,
    OpcUa_UInt32                            a_nSubscriptionId,
    OpcUa_TimestampsToReturn                a_eTimestampsToReturn,
    OpcUa_Int32                             a_nNoOfItemsToCreate,
    const OpcUa_MonitoredItemCreateRequest* a_pItemsToCreate,
    OpcUa_ResponseHeader*                   a_pResponseHeader,
    OpcUa_Int32*                            a_pNoOfResults,
    OpcUa_MonitoredItemCreateResult**       a_pResults,
    OpcUa_Int32*                            a_pNoOfDiagnosticInfos,
    OpcUa_DiagnosticInfo**                  a_pDiagnosticInfos)
{
    OpcUa_Int32 i;
    OpcUa_Void* p_Node;
    MySubscription* pSubscription;
    MyMonitoredItem* pMonitoredItem;
    SessionData* pSession = OpcUa_Null;

    OpcUa_InitializeStatus(OpcUa_Module_Server, "OpcUa_ServerApi_CreateMonitoredItems");

    /* Validate arguments. */
    OpcUa_ReturnErrorIfArgumentNull(a_hEndpoint);
    OpcUa_ReturnErrorIfArgumentNull(a_hContext);
    OpcUa_ReturnErrorIfArgumentNull(a_pRequestHeader);
    OpcUa_ReferenceParameter(a_nSubscriptionId);
    OpcUa_ReferenceParameter(a_eTimestampsToReturn);
    OpcUa_ReturnErrorIfArrayArgumentNull(a_nNoOfItemsToCreate, a_pItemsToCreate);
    OpcUa_ReturnErrorIfArgumentNull(a_pResponseHeader);
    OpcUa_ReturnErrorIfArrayArgumentNull(a_pNoOfResults, a_pResults);
    OpcUa_ReturnErrorIfArrayArgumentNull(a_pNoOfDiagnosticInfos, a_pDiagnosticInfos);

    *a_pNoOfDiagnosticInfos = 0;
    *a_pDiagnosticInfos = OpcUa_Null;

#ifndef NO_DEBUGGING_
    MY_TRACE("\n\n\nCREATEMONITOREDITEMS SERVICE ON SESSION %d SUBSCRIPTION %d==============================================\n",
        a_pRequestHeader->AuthenticationToken.Identifier.Numeric, a_nSubscriptionId);
#endif /*_DEBUGGING_*/

    OpcUa_Mutex_Lock(UaTestServer_g_hSubscriptionMutex);

    pSession = UaTestServer_Session_Find(&a_pRequestHeader->AuthenticationToken);
    OpcUa_GotoErrorIfNull(pSession, OpcUa_BadSecurityChecksFailed);

    RESET_SESSION_COUNTER(pSession);

    pSubscription = find_subscription(a_nSubscriptionId);
    if (pSubscription == OpcUa_Null)
    {
        uStatus = OpcUa_BadSubscriptionIdInvalid;
        OpcUa_GotoError;
    }

    *a_pResults = OpcUa_Alloc(a_nNoOfItemsToCreate * sizeof(OpcUa_MonitoredItemCreateResult));
    OpcUa_GotoErrorIfAllocFailed(*a_pResults);

    for (i = 0; i < a_nNoOfItemsToCreate; i++)
    {
        const OpcUa_MonitoredItemCreateRequest* request = &a_pItemsToCreate[i];
        OpcUa_MonitoredItemCreateResult_Initialize((*a_pResults) + i);

        const OpcUa_ReadValueId* value = &request->ItemToMonitor;
        p_Node = search_for_node(value->NodeId);
        if (p_Node == OpcUa_Null)
        {
            (*a_pResults)[i].StatusCode = OpcUa_BadNodeIdUnknown;
            continue;
        }

        if ((value->AttributeId != OpcUa_Attributes_EventNotifier) &&
            (value->AttributeId != OpcUa_Attributes_Value))
        {
            (*a_pResults)[i].StatusCode = OpcUa_BadAttributeIdInvalid;
            continue;
        }

        (*a_pResults)[i].StatusCode = add_monitored_item(
            pSubscription,
            value->AttributeId,
            value->NodeId,
            request->RequestedParameters.ClientHandle, 
            a_eTimestampsToReturn,
            &pMonitoredItem);

        if (!OpcUa_IsBad((*a_pResults)[i].StatusCode))
        {
            (*a_pResults)[i].MonitoredItemId = pMonitoredItem->Id;
            (*a_pResults)[i].RevisedSamplingInterval = PUBLISHING_INTERVAL_MS; /* request->RequestedParameters.SamplingInterval */
            (*a_pResults)[i].RevisedQueueSize = 1;
        }

        MY_TRACE("Monitor NodeId |%d|  NamespaceIndex |%d|  AttributeId |%d|\n",
            ((_ObjectNode_*)p_Node)->BaseAttribute.NodeId.Identifier.Numeric,
            ((_ObjectNode_*)p_Node)->BaseAttribute.NodeId.NamespaceIndex,
            value->AttributeId);
    }

    *a_pNoOfResults = a_nNoOfItemsToCreate;

    uStatus = response_header_fill(pSession, a_pResponseHeader, a_pRequestHeader, uStatus);
    if (OpcUa_IsBad(uStatus))
    {
        a_pResponseHeader->ServiceResult = OpcUa_BadInternalError;
    }

#ifndef NO_DEBUGGING_
    MY_TRACE("\nSERVICE===END============================================\n\n\n");
#endif /*_DEBUGGING_*/

    OpcUa_Mutex_Unlock(UaTestServer_g_hSubscriptionMutex);

    OpcUa_ReturnStatusCode;
    OpcUa_BeginErrorHandling;

    uStatus = response_header_fill(pSession, a_pResponseHeader, a_pRequestHeader, uStatus);
    if (OpcUa_IsBad(uStatus))
    {
        a_pResponseHeader->ServiceResult = OpcUa_BadInternalError;
    }
#ifndef NO_DEBUGGING_
    MY_TRACE("\nSERVICE END (WITH ERROR)===========\n\n\n");
#endif /*_DEBUGGING_*/

    OpcUa_Mutex_Unlock(UaTestServer_g_hSubscriptionMutex);

    OpcUa_FinishErrorHandling;
}

/*============================================================================
 * A method that implements the CreateMonitoredItems service.
 *===========================================================================*/
OpcUa_StatusCode my_DeleteMonitoredItems(
    OpcUa_Endpoint             a_hEndpoint,
    OpcUa_Handle               a_hContext,
    const OpcUa_RequestHeader* a_pRequestHeader,
    OpcUa_UInt32               a_nSubscriptionId,
    OpcUa_Int32                a_nNoOfMonitoredItemIds,
    const OpcUa_UInt32*        a_pMonitoredItemIds,
    OpcUa_ResponseHeader*      a_pResponseHeader,
    OpcUa_Int32*               a_pNoOfResults,
    OpcUa_StatusCode**         a_pResults,
    OpcUa_Int32*               a_pNoOfDiagnosticInfos,
    OpcUa_DiagnosticInfo**     a_pDiagnosticInfos)
{
    OpcUa_Int32 i;
    MySubscription* pSubscription;
    MyMonitoredItem* pMonitoredItem;
    SessionData* pSession = OpcUa_Null;

    OpcUa_InitializeStatus(OpcUa_Module_Server, "OpcUa_ServerApi_DeleteMonitoredItems");

    /* Validate arguments. */
    OpcUa_ReturnErrorIfArgumentNull(a_hEndpoint);
    OpcUa_ReturnErrorIfArgumentNull(a_hContext);
    OpcUa_ReturnErrorIfArgumentNull(a_pRequestHeader);
    OpcUa_ReferenceParameter(a_nSubscriptionId);
    OpcUa_ReturnErrorIfArrayArgumentNull(a_nNoOfMonitoredItemIds, a_pMonitoredItemIds);
    OpcUa_ReturnErrorIfArgumentNull(a_pResponseHeader);
    OpcUa_ReturnErrorIfArrayArgumentNull(a_pNoOfResults, a_pResults);
    OpcUa_ReturnErrorIfArrayArgumentNull(a_pNoOfDiagnosticInfos, a_pDiagnosticInfos);

#ifndef NO_DEBUGGING_
    MY_TRACE("\n\n\nDELETEMONITOREDITEMS SERVICE ON SESSION %d SUBSCRIPTION %d==============================================\n",
        a_pRequestHeader->AuthenticationToken.Identifier.Numeric, a_nSubscriptionId);
#endif /*_DEBUGGING_*/

    OpcUa_Mutex_Lock(UaTestServer_g_hSubscriptionMutex);

    pSession = UaTestServer_Session_Find(&a_pRequestHeader->AuthenticationToken);
    OpcUa_GotoErrorIfNull(pSession, OpcUa_BadSecurityChecksFailed);

    RESET_SESSION_COUNTER(pSession);

    pSubscription = find_subscription(a_nSubscriptionId);
    if (pSubscription == OpcUa_Null)
    {
        uStatus = OpcUa_BadSubscriptionIdInvalid;
        OpcUa_GotoError;
    }

    *a_pResults = OpcUa_Alloc(a_nNoOfMonitoredItemIds * sizeof(OpcUa_StatusCode));
    OpcUa_GotoErrorIfAllocFailed(*a_pResults);
    *a_pNoOfResults = a_nNoOfMonitoredItemIds;

    for (i = 0; i < a_nNoOfMonitoredItemIds; i++)
    {
        pMonitoredItem = find_monitored_item(pSubscription, a_pMonitoredItemIds[i]);
        if (pMonitoredItem == OpcUa_Null)
        {
            (*a_pResults)[i] = OpcUa_BadMonitoredItemIdInvalid;
            continue;
        }

        MY_TRACE("Stopping monitoring NodeId |%d|  NamespaceIndex |%d|\n",
            pMonitoredItem->NodeId.Identifier.Numeric,
            pMonitoredItem->NodeId.NamespaceIndex);

        delete_monitored_item(pMonitoredItem);
        (*a_pResults)[i] = OpcUa_Good;
    }

    uStatus = response_header_fill(pSession, a_pResponseHeader, a_pRequestHeader, uStatus);
    if (OpcUa_IsBad(uStatus))
    {
        a_pResponseHeader->ServiceResult = OpcUa_BadInternalError;
    }

#ifndef NO_DEBUGGING_
    MY_TRACE("\nSERVICE===END============================================\n\n\n");
#endif /*_DEBUGGING_*/

    OpcUa_Mutex_Unlock(UaTestServer_g_hSubscriptionMutex);

    OpcUa_ReturnStatusCode;
    OpcUa_BeginErrorHandling;

    uStatus = response_header_fill(pSession, a_pResponseHeader, a_pRequestHeader, uStatus);
    if (OpcUa_IsBad(uStatus))
    {
        a_pResponseHeader->ServiceResult = OpcUa_BadInternalError;
    }
#ifndef NO_DEBUGGING_
    MY_TRACE("\nSERVICE END (WITH ERROR)===========\n\n\n");
#endif /*_DEBUGGING_*/

    OpcUa_Mutex_Unlock(UaTestServer_g_hSubscriptionMutex);

    OpcUa_FinishErrorHandling;
}

/*============================================================================
* Begins processing of a Publish service request.  We use the async version
* because the spec requires us to not complete the Publish request until we
* actually have something to return to the client.
*===========================================================================*/
OpcUa_StatusCode my_BeginPublish(
    OpcUa_Endpoint        a_hEndpoint,
    OpcUa_Handle          a_hContext,
    OpcUa_Void**          a_ppRequest,
    OpcUa_EncodeableType* a_pRequestType)
{
    OpcUa_PublishRequest* pRequest = OpcUa_Null;
    OpcUa_PublishResponse* pResponse = OpcUa_Null;
    MyPublishQueueItem* pPublishQueueItem = OpcUa_Null;
    OpcUa_Int32 i;
    SessionData* pSession = OpcUa_Null;

    OpcUa_InitializeStatus(OpcUa_Module_Server, "OpcUa_Server_BeginPublish");

    OpcUa_ReturnErrorIfArgumentNull(a_hEndpoint);
    OpcUa_ReturnErrorIfArgumentNull(a_hContext);
    OpcUa_ReturnErrorIfArgumentNull(a_ppRequest);
    OpcUa_ReturnErrorIfArgumentNull(*a_ppRequest);
    OpcUa_ReturnErrorIfArgumentNull(a_pRequestType);
    OpcUa_ReturnErrorIfTrue(a_pRequestType->TypeId != OpcUaId_PublishRequest, OpcUa_BadInvalidArgument);

    pRequest = (OpcUa_PublishRequest*)*a_ppRequest;

#ifndef NO_DEBUGGING_
    MY_TRACE("\n\n\nPUBLISH SERVICE ON SESSION %d==============================================\n",
        pRequest->RequestHeader.AuthenticationToken.Identifier.Numeric);
#endif /*_DEBUGGING_*/

    OpcUa_Mutex_Lock(UaTestServer_g_hSubscriptionMutex);

    pSession = UaTestServer_Session_Find(&pRequest->RequestHeader.AuthenticationToken);
    OpcUa_GotoErrorIfNull(pSession, OpcUa_BadSecurityChecksFailed);

    RESET_SESSION_COUNTER(pSession);

    pPublishQueueItem = create_publish_queue_item(
        a_hEndpoint,
        a_hContext,
        (OpcUa_PublishRequest**)a_ppRequest,
        a_pRequestType);
    OpcUa_ReturnErrorIfAllocFailed(pPublishQueueItem);
    pResponse = pPublishQueueItem->pResponse;

    if (OpcUa_IsBad(pSession->session_flag))
    {
#ifndef NO_DEBUGGING_
        MY_TRACE("\nSession not active\n");
#endif /*_DEBUGGING_*/
        uStatus = OpcUa_BadSessionNotActivated;
        OpcUa_GotoError;
    }

    pResponse->Results = OpcUa_Alloc(pRequest->NoOfSubscriptionAcknowledgements * sizeof(OpcUa_StatusCode));
    OpcUa_GotoErrorIfAllocFailed(pResponse->Results);
    pResponse->NoOfResults = pRequest->NoOfSubscriptionAcknowledgements;

    /* Process acknowledgements for previously sent notifications. */
    for (i = 0; i < pRequest->NoOfSubscriptionAcknowledgements; i++)
    {
        OpcUa_UInt32 uId = pRequest->SubscriptionAcknowledgements[i].SubscriptionId;
        MySubscription* pSubscription = find_subscription(uId);
        if (pSubscription == OpcUa_Null)
        {
            pResponse->Results[i] = OpcUa_BadSubscriptionIdInvalid;
            continue;
        }

        /* Remove SequenceNumber from queue.  Currently we only support a queue size of 1. */
        if (pSubscription->SeqNum != pRequest->SubscriptionAcknowledgements[i].SequenceNumber)
        {
            pResponse->Results[i] = OpcUa_BadSequenceNumberUnknown;
            continue;
        }
        pSubscription->LastSequenceNumberAcknowledged = pRequest->SubscriptionAcknowledgements[i].SequenceNumber;

        pPublishQueueItem->pResponse->Results[i] = OpcUa_Good;
    }

    if (OpcUa_IsBad(uStatus))
    {
        OpcUa_Void* pFault = OpcUa_Null;
        OpcUa_EncodeableType* pFaultType = OpcUa_Null;

        uStatus = OpcUa_ServerApi_CreateFault(
            &pRequest->RequestHeader,
            uStatus,
            &pResponse->ResponseHeader.ServiceDiagnostics,
            &pResponse->ResponseHeader.NoOfStringTable,
            &pResponse->ResponseHeader.StringTable,
            &pFault,
            &pFaultType);

        OpcUa_GotoErrorIfBad(uStatus);

        /* Free the response. */
        OpcUa_EncodeableObject_Delete(pPublishQueueItem->pResponseType, (OpcUa_Void**)&pPublishQueueItem->pResponse);

        /* Make the response the fault. */
        pPublishQueueItem->pResponse = pResponse = (OpcUa_PublishResponse*)pFault;
        pPublishQueueItem->pResponseType = pFaultType;

        my_CompletePublish(pPublishQueueItem, OpcUa_Null, OpcUa_Good);
    }
    else
    {
        /* See if we have any immediate notifications to send. */
        MySubscription* pSubscription = get_changed_subscription(pSession, OpcUa_False);
        if (pSubscription != OpcUa_Null)
        {
            my_CompletePublish(pPublishQueueItem, pSubscription, OpcUa_Good);

#ifndef NO_DEBUGGING_
            MY_TRACE("\nSERVICE END===========\n\n\n");
#endif /*_DEBUGGING_*/
        }
        else
        {
            uStatus = my_EnqueuePublishingReq(pPublishQueueItem);
            OpcUa_GotoErrorIfBad(uStatus);

#ifndef NO_DEBUGGING_
    MY_TRACE("\nSERVICE END (QUEUED)===========\n\n\n");
#endif /*_DEBUGGING_*/
        }
    }

    OpcUa_Mutex_Unlock(UaTestServer_g_hSubscriptionMutex);

    OpcUa_ReturnStatusCode;
    OpcUa_BeginErrorHandling;

#ifndef NO_DEBUGGING_
    MY_TRACE("\nSERVICE END (WITH ERROR)===========\n\n\n");
#endif /*_DEBUGGING_*/

    my_CompletePublish(pPublishQueueItem, OpcUa_Null, uStatus);

    OpcUa_Mutex_Unlock(UaTestServer_g_hSubscriptionMutex);

    OpcUa_FinishErrorHandling;
}

/*============================================================================
 * Begins processing of a Republish service request.
 *===========================================================================*/
OpcUa_StatusCode my_BeginRepublish(
    OpcUa_Endpoint        a_hEndpoint,
    OpcUa_Handle          a_hContext,
    OpcUa_Void**          a_ppRequest,
    OpcUa_EncodeableType* a_pRequestType)
{
    OpcUa_RepublishRequest* pRequest = OpcUa_Null;
    OpcUa_RepublishResponse* pResponse = OpcUa_Null;
    OpcUa_EncodeableType* pResponseType = OpcUa_Null;
    SessionData* pSession = OpcUa_Null;
    OpcUa_Void* pFault = OpcUa_Null;
    OpcUa_EncodeableType* pFaultType = OpcUa_Null;

OpcUa_InitializeStatus(OpcUa_Module_Server, "OpcUa_Server_BeginRepublish");

    OpcUa_ReturnErrorIfArgumentNull(a_hEndpoint);
    OpcUa_ReturnErrorIfArgumentNull(a_hContext);
    OpcUa_ReturnErrorIfArgumentNull(a_ppRequest);
    OpcUa_ReturnErrorIfArgumentNull(*a_ppRequest);
    OpcUa_ReturnErrorIfArgumentNull(a_pRequestType);

    OpcUa_ReturnErrorIfTrue(a_pRequestType->TypeId != OpcUaId_RepublishRequest, OpcUa_BadInvalidArgument);

    pRequest = (OpcUa_RepublishRequest*)*a_ppRequest;

#ifndef NO_DEBUGGING_
    MY_TRACE("\n\n\nREPUBLISH SERVICE ON SESSION %d==============================================\n",
        pRequest->RequestHeader.AuthenticationToken.Identifier.Numeric);
#endif /*_DEBUGGING_*/

    /* Create a context to use for sending a response. */
    uStatus = OpcUa_Endpoint_BeginSendResponse(a_hEndpoint, a_hContext, (OpcUa_Void**)&pResponse, &pResponseType);
    OpcUa_GotoErrorIfBad(uStatus);

    OpcUa_Mutex_Lock(UaTestServer_g_hSubscriptionMutex);

    pSession = UaTestServer_Session_Find(&pRequest->RequestHeader.AuthenticationToken);
    OpcUa_GotoErrorIfNull(pSession, OpcUa_BadSecurityChecksFailed);

    RESET_SESSION_COUNTER(pSession);
    
    MySubscription* pSubscription = find_subscription(pRequest->SubscriptionId);
    OpcUa_GotoErrorIfNull(pSubscription, OpcUa_BadSubscriptionIdInvalid);

    my_ResetLifetimeCounter(pSubscription);

    /* We currently don't support retransmitting notification messages. */
#ifndef NO_DEBUGGING_
    MY_TRACE("Retransmitting notifications not currently supported\n");
#endif /*_DEBUGGING_*/
    uStatus = OpcUa_BadMessageNotAvailable;
    OpcUa_GotoError;


OpcUa_ReturnStatusCode;
OpcUa_BeginErrorHandling;

    /* Send an error response. */
    uStatus = OpcUa_ServerApi_CreateFault(
        &pRequest->RequestHeader,
        uStatus,
        &pResponse->ResponseHeader.ServiceDiagnostics,
        &pResponse->ResponseHeader.NoOfStringTable,
        &pResponse->ResponseHeader.StringTable,
        &pFault,
        &pFaultType);

    /* Free the response. */
    OpcUa_EncodeableObject_Delete(pResponseType, (OpcUa_Void**)&pResponse);

    /* Make the response the fault. */
    pResponse = (OpcUa_RepublishResponse*)pFault;
    pResponseType = pFaultType;

    uStatus = response_header_fill(pSession, &pResponse->ResponseHeader, &pRequest->RequestHeader, uStatus);
    if (OpcUa_IsBad(uStatus))
    {
        pResponse->ResponseHeader.ServiceResult = OpcUa_BadInternalError;
    }

    OpcUa_Endpoint_EndSendResponse(
        a_hEndpoint,
        &a_hContext,
        OpcUa_Good,
        pResponse,
        pResponseType);

    OpcUa_EncodeableObject_Delete(pResponseType, (OpcUa_Void**)&pResponse);

#ifndef NO_DEBUGGING_
    MY_TRACE("\nSERVICE END (WITH ERROR)===========\n\n\n");
#endif /*_DEBUGGING_*/
    OpcUa_Mutex_Unlock(UaTestServer_g_hSubscriptionMutex);

OpcUa_FinishErrorHandling;
}

/*===========================================================================================*/
/** @brief Create an ExtensionObject from an EncodeableType                                  */
/*===========================================================================================*/

void* OpcUa_ExtensionObject_CreateFromType(OpcUa_ExtensionObject* a_pExtension, OpcUa_EncodeableType* a_pType)
{
    if ((a_pExtension == OpcUa_Null) || (a_pType == OpcUa_Null))
    {
        return OpcUa_Null;
    }

    OpcUa_StatusCode uStatus = OpcUa_EncodeableObject_Create(a_pType, &a_pExtension->Body.EncodeableObject.Object);

    if (OpcUa_IsBad(uStatus))
    {
        return OpcUa_Null;
    }

    a_pExtension->TypeId.NodeId.IdentifierType = OpcUa_IdentifierType_Numeric;
    a_pExtension->TypeId.NodeId.Identifier.Numeric = a_pType->BinaryEncodingTypeId;
    a_pExtension->Encoding = OpcUa_ExtensionObjectEncoding_EncodeableObject;
    a_pExtension->Body.EncodeableObject.Type = a_pType;

    return a_pExtension->Body.EncodeableObject.Object;
}

/*============================================================================
 * Complete processing of a Publish service request by sending either a 
 * keepalive or a set of notifications.
 *===========================================================================*/
OpcUa_StatusCode my_CompletePublish(
    MyPublishQueueItem* a_pPublishQueueItem,
    MySubscription*     a_pSubscription,
    OpcUa_StatusCode    a_uStatus)
{
    OpcUa_PublishRequest* pRequest = a_pPublishQueueItem->pRequest;
    OpcUa_PublishResponse* pResponse = a_pPublishQueueItem->pResponse;
    SessionData* pSession = OpcUa_Null;

    OpcUa_InitializeStatus(OpcUa_Module_Server, "my_CompletePublish");

    if (a_pSubscription != OpcUa_Null)
    {
        pSession = a_pSubscription->pSession;

#ifndef NO_DEBUGGING_
        MY_TRACE("\n\n\n(COMPLETE) PUBLISH ON SESSION %d SUBSCRIPTION %d==============================================\n",
            pSession->SessionId.Identifier.Numeric, a_pSubscription->Id);
#endif /*_DEBUGGING_*/
    }

    uStatus = a_uStatus;
    OpcUa_GotoErrorIfBad(uStatus);

    /* Now send any new notifications. */
    if (a_pSubscription == OpcUa_Null)
    {
        /* Send an error. */
        pResponse->SubscriptionId = 0; /* none */
        pResponse->MoreNotifications = OpcUa_False;
    }
    else
    {
        MyMonitoredItem* pMonitoredItem;

        /* We currently only support data notifications.  If we later support
         * event notifications, this might be 2 if both types need to be sent. */
        const OpcUa_UInt32 notificationTypes = 1;

        pResponse->SubscriptionId = a_pSubscription->Id;
        pResponse->NotificationMessage.SequenceNumber = ++a_pSubscription->SeqNum;
        pResponse->MoreNotifications = OpcUa_False;
#ifndef NO_DEBUGGING_
        MY_TRACE("SequenceNumber: %d\n", pResponse->NotificationMessage.SequenceNumber);
#endif /*_DEBUGGING_*/

        /* Create a data notification. */
        OpcUa_ExtensionObject* obj = (OpcUa_ExtensionObject*)
            OpcUa_Alloc(notificationTypes * sizeof(*obj));
        OpcUa_GotoErrorIfAllocFailed(obj);
        OpcUa_ExtensionObject_Initialize(obj);

        /* Create an extension object container for it. */
        OpcUa_DataChangeNotification* dataChangeNotification = OpcUa_ExtensionObject_CreateFromType(
            &obj[0],
            &OpcUa_DataChangeNotification_EncodeableType);
        if (obj == OpcUa_Null)
        {
            OpcUa_Free(obj);
            OpcUa_GotoErrorIfAllocFailed(dataChangeNotification);
        }
        OpcUa_DataChangeNotification_Initialize(dataChangeNotification);
        pResponse->NotificationMessage.NotificationData = obj;
        pResponse->NotificationMessage.NoOfNotificationData = notificationTypes;

        if (a_pSubscription->NotificationsAvailable > 0)
        {
            OpcUa_UInt32 count = 0;

            dataChangeNotification->MonitoredItems =
                OpcUa_Alloc(a_pSubscription->NotificationsAvailable * sizeof(OpcUa_MonitoredItemNotification));
            OpcUa_GotoErrorIfAllocFailed(dataChangeNotification->MonitoredItems);
            OpcUa_MemSet(dataChangeNotification->MonitoredItems, 0, a_pSubscription->NotificationsAvailable * sizeof(OpcUa_MonitoredItemNotification));

            /* Fill in notifications, if any. */
            for (pMonitoredItem = a_pSubscription->MonitoredItemsHead.Next;
                pMonitoredItem != &a_pSubscription->MonitoredItemsHead;
                pMonitoredItem = pMonitoredItem->Next)
            {
                if (pMonitoredItem->Dirty)
                {
                    OpcUa_MonitoredItemNotification* notification = &dataChangeNotification->MonitoredItems[count];
                    OpcUa_MonitoredItemNotification_Initialize(notification);

                    uStatus = copy_data_value(&notification->Value, &pMonitoredItem->LastValue);
                    OpcUa_GotoErrorIfBad(uStatus);

                    notification->ClientHandle = pMonitoredItem->ClientHandle;

#ifndef NO_DEBUGGING_
                    MY_TRACE("Publish NodeId |%d|  NamespaceIndex |%d|\n",
                             pMonitoredItem->NodeId.Identifier.Numeric,
                             pMonitoredItem->NodeId.NamespaceIndex);
#endif /*_DEBUGGING_*/

                    count++;

                    pMonitoredItem->Dirty = OpcUa_False;
                }
            }

            dataChangeNotification->NoOfMonitoredItems = count;
        }
    }
    pResponse->NoOfAvailableSequenceNumbers = 0;
    pResponse->AvailableSequenceNumbers = OpcUa_Null;
    pResponse->NotificationMessage.PublishTime = OpcUa_DateTime_UtcNow();

    uStatus = response_header_fill(pSession, &pResponse->ResponseHeader, &pRequest->RequestHeader, uStatus);
    if (OpcUa_IsBad(uStatus))
    {
        pResponse->ResponseHeader.ServiceResult = OpcUa_BadInternalError;
    }

    OpcUa_Endpoint_EndSendResponse(
        a_pPublishQueueItem->hEndpoint, 
        &a_pPublishQueueItem->hContext,
        OpcUa_Good,
        a_pPublishQueueItem->pResponse,
        a_pPublishQueueItem->pResponseType);

    free_publish_queue_item(a_pPublishQueueItem);

#ifndef NO_DEBUGGING_
    MY_TRACE("\nCOMPLETE PUBLISH===END============================================\n\n\n");
#endif /*_DEBUGGING_*/

    OpcUa_ReturnStatusCode;
    OpcUa_BeginErrorHandling;

    /* Send an error response. */
    OpcUa_Endpoint_EndSendResponse(
        a_pPublishQueueItem->hEndpoint, 
        &a_pPublishQueueItem->hContext,
        uStatus,
        OpcUa_Null,
        OpcUa_Null);

    free_publish_queue_item(a_pPublishQueueItem);

    OpcUa_FinishErrorHandling;
}

/*============================================================================
 * A stub method which implements the SetPublishingMode service.
 *===========================================================================*/
OpcUa_StatusCode my_SetPublishingMode(
    OpcUa_Endpoint             a_hEndpoint,
    OpcUa_Handle               a_hContext,
    const OpcUa_RequestHeader* a_pRequestHeader,
    OpcUa_Boolean              a_bPublishingEnabled,
    OpcUa_Int32                a_nNoOfSubscriptionIds,
    const OpcUa_UInt32*        a_pSubscriptionIds,
    OpcUa_ResponseHeader*      a_pResponseHeader,
    OpcUa_Int32*               a_pNoOfResults,
    OpcUa_StatusCode**         a_pResults,
    OpcUa_Int32*               a_pNoOfDiagnosticInfos,
    OpcUa_DiagnosticInfo**     a_pDiagnosticInfos)
{
    OpcUa_Int32 i;
    SessionData* pSession = OpcUa_Null;

OpcUa_InitializeStatus(OpcUa_Module_Server, "OpcUa_ServerApi_SetPublishingMode");

    /* Validate arguments. */
    OpcUa_ReturnErrorIfArgumentNull(a_hEndpoint);
    OpcUa_ReturnErrorIfArgumentNull(a_hContext);
    OpcUa_ReturnErrorIfArgumentNull(a_pRequestHeader);
    OpcUa_ReferenceParameter(a_bPublishingEnabled);
    OpcUa_ReturnErrorIfArrayArgumentNull(a_nNoOfSubscriptionIds, a_pSubscriptionIds);
    OpcUa_ReturnErrorIfArgumentNull(a_pResponseHeader);
    OpcUa_ReturnErrorIfArgumentNull(a_pNoOfResults);
    OpcUa_ReturnErrorIfArgumentNull(a_pResults);
    OpcUa_ReturnErrorIfArgumentNull(a_pNoOfDiagnosticInfos);
    OpcUa_ReturnErrorIfArgumentNull(a_pDiagnosticInfos);

#ifndef NO_DEBUGGING_
    MY_TRACE("\n\n\nSETPUBLISHINGMODE SERVICE ON SESSION %d==============================================\n",
        a_pRequestHeader->AuthenticationToken.Identifier.Numeric);
    MY_TRACE("PublishingEnabled: %d\n", a_bPublishingEnabled);
#endif /*_DEBUGGING_*/

    OpcUa_Mutex_Lock(UaTestServer_g_hSubscriptionMutex);

    pSession = UaTestServer_Session_Find(&a_pRequestHeader->AuthenticationToken);
    OpcUa_GotoErrorIfNull(pSession, OpcUa_BadSecurityChecksFailed);

    RESET_SESSION_COUNTER(pSession);

    *a_pResults = OpcUa_Alloc(a_nNoOfSubscriptionIds * sizeof(OpcUa_StatusCode));
    OpcUa_GotoErrorIfAllocFailed(*a_pResults);

    for (i = 0; i < a_nNoOfSubscriptionIds; i++)
    {
        OpcUa_UInt32 id = a_pSubscriptionIds[i];

        MySubscription* pSubscription = find_subscription(id);
        if (pSubscription == OpcUa_Null)
        {
            (*a_pResults)[i] = OpcUa_BadSubscriptionIdInvalid;
        }
        else
        {
            pSubscription->PublishingEnabled = a_bPublishingEnabled;
            (*a_pResults)[i] = OpcUa_Good;
        }
    }

    *a_pNoOfResults = a_nNoOfSubscriptionIds;

    uStatus = response_header_fill(pSession, a_pResponseHeader, a_pRequestHeader, uStatus);
    if (OpcUa_IsBad(uStatus))
    {
        a_pResponseHeader->ServiceResult = OpcUa_BadInternalError;
    }

#ifndef NO_DEBUGGING_
    MY_TRACE("\nSERVICE===END============================================\n\n\n");
#endif /*_DEBUGGING_*/

    OpcUa_Mutex_Unlock(UaTestServer_g_hSubscriptionMutex);

OpcUa_ReturnStatusCode;
OpcUa_BeginErrorHandling;

    uStatus = response_header_fill(pSession, a_pResponseHeader, a_pRequestHeader, uStatus);
    if (OpcUa_IsBad(uStatus))
    {
        a_pResponseHeader->ServiceResult = OpcUa_BadInternalError;
    }
#ifndef NO_DEBUGGING_
    MY_TRACE("\nSERVICE END (WITH ERROR)===========\n\n\n");
#endif /*_DEBUGGING_*/

    OpcUa_Mutex_Unlock(UaTestServer_g_hSubscriptionMutex);

OpcUa_FinishErrorHandling;
}
