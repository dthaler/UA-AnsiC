/* ========================================================================
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
 
#ifndef _subscriptionservice_
#define _subscriptionservice_

extern OpcUa_Mutex UaTestServer_g_hSubscriptionMutex;

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
    OpcUa_UInt32*              a_pRevisedMaxKeepAliveCount);

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
    OpcUa_DiagnosticInfo**     a_pDiagnosticInfos);
    

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
    OpcUa_DiagnosticInfo**                  a_pDiagnosticInfos);

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
    OpcUa_DiagnosticInfo**     a_pDiagnosticInfos);

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
    OpcUa_DiagnosticInfo**     a_pDiagnosticInfos);

OpcUa_StatusCode my_BeginPublish(
    OpcUa_Endpoint        a_hEndpoint,
    OpcUa_Handle          a_hContext,
    OpcUa_Void**          a_ppRequest,
    OpcUa_EncodeableType* a_pRequestType);

OpcUa_StatusCode my_BeginRepublish(
    OpcUa_Endpoint        a_hEndpoint,
    OpcUa_Handle          a_hContext,
    OpcUa_Void**          a_ppRequest,
    OpcUa_EncodeableType* a_pRequestType);

OpcUa_Void delete_all_subscriptions(SessionData* a_pSession);

#endif /*_subscriptionservice_*/
