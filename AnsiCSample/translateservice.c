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
 
#include <opcua.h>
#include <opcua_serverstub.h>
#include <opcua_string.h>
#include <opcua_memory.h>

#include "addressspace.h"
#include "translateservice.h"
#include "mytrace.h"
#include "general_header.h"
#include "browseservice.h"

/*============================================================================
 * A stub method which implements the TranslateBrowsePathsToNodeIds service.
 *===========================================================================*/
OpcUa_StatusCode my_TranslateBrowsePathsToNodeIds(
    OpcUa_Endpoint             a_hEndpoint,
    OpcUa_Handle               a_hContext,
    const OpcUa_RequestHeader* a_pRequestHeader,
    OpcUa_Int32                a_nNoOfBrowsePaths,
    const OpcUa_BrowsePath*    a_pBrowsePaths,
    OpcUa_ResponseHeader*      a_pResponseHeader,
    OpcUa_Int32*               a_pNoOfResults,
    OpcUa_BrowsePathResult**   a_pResults,
    OpcUa_Int32*               a_pNoOfDiagnosticInfos,
    OpcUa_DiagnosticInfo**     a_pDiagnosticInfos)
{
    OpcUa_Int i, level;
    OpcUa_Void* p_Node = OpcUa_Null, *p_Parent;
    OpcUa_StringA namespaceUri;
    SessionData* pSession = OpcUa_Null;

    OpcUa_InitializeStatus(OpcUa_Module_Server, "OpcUa_ServerApi_TranslateBrowsePathsToNodeIds");

    /* validate arguments. */
    OpcUa_ReturnErrorIfArgumentNull(a_hEndpoint);
    OpcUa_ReturnErrorIfArgumentNull(a_hContext);
    OpcUa_ReturnErrorIfArgumentNull(a_pRequestHeader);
    OpcUa_ReturnErrorIfArrayArgumentNull(a_nNoOfBrowsePaths, a_pBrowsePaths);
    OpcUa_ReturnErrorIfArgumentNull(a_pResponseHeader);
    OpcUa_ReturnErrorIfArrayArgumentNull(a_pNoOfResults, a_pResults);
    OpcUa_ReturnErrorIfArrayArgumentNull(a_pNoOfDiagnosticInfos, a_pDiagnosticInfos);

    *a_pNoOfDiagnosticInfos = 0;
    *a_pDiagnosticInfos = OpcUa_Null;

#ifndef NO_DEBUGGING_
    MY_TRACE("\n\n\nTRANSLATEBROWSEPATHSTONODEIDS SERVICE==============================================\n");
    MY_TRACE("\nnumber of paths: %d\n", a_nNoOfBrowsePaths);
#endif /*_DEBUGGING_*/

    pSession = UaTestServer_Session_Find(&a_pRequestHeader->AuthenticationToken);
    OpcUa_GotoErrorIfNull(pSession, OpcUa_BadSecurityChecksFailed);

    *a_pResults = OpcUa_Alloc(a_nNoOfBrowsePaths * sizeof(OpcUa_BrowsePathResult));
    OpcUa_GotoErrorIfAllocFailed(*a_pResults);

    for (i = 0; i < a_nNoOfBrowsePaths; i++)
    {
        MY_TRACE("%s", getNodeIdString(&a_pBrowsePaths[i].StartingNode));

        OpcUa_BrowsePathResult_Initialize((*a_pResults) + i);

        (*a_pResults)[i].StatusCode = OpcUa_BadNoMatch;

        /* We currently only support one target per browse name. */
        (*a_pResults)[i].Targets = OpcUa_Alloc(sizeof(OpcUa_BrowsePathTarget));
        OpcUa_GotoErrorIfAllocFailed((*a_pResults)[i].Targets);
        (*a_pResults)[i].NoOfTargets = 1;

        OpcUa_BrowsePathTarget_Initialize(&(*a_pResults)[i].Targets[0]);

        /* Fill in target. */
        p_Parent = OpcUa_Null;
        for (level = 0; level < a_pBrowsePaths[i].RelativePath.NoOfElements; level++)
        {
            if (level > 0)
            {
                MY_TRACE(".");
            }
            MY_TRACE("%s", OpcUa_String_GetRawString(&a_pBrowsePaths[i].RelativePath.Elements[level].TargetName.Name));

            p_Node = search_for_node_by_path(p_Parent, OpcUa_String_GetRawString(&a_pBrowsePaths[i].RelativePath.Elements[level].TargetName.Name), &namespaceUri);
            if (p_Node == OpcUa_Null)
            {
                break;
            }
            p_Parent = p_Node;
            
            MY_TRACE("(%s)", getNodeIdString(&((_ObjectNode_*)p_Node)->BaseAttribute.NodeId));
        }
        MY_TRACE("\n");

        if (level < a_pBrowsePaths[i].RelativePath.NoOfElements)
        {
            continue;
        }

        (*a_pResults)[i].StatusCode = OpcUa_Good;
        (*a_pResults)[i].Targets[0].RemainingPathIndex = (OpcUa_UInt32)-1;
        (*a_pResults)[i].Targets[0].TargetId.ServerIndex = 0;
        (*a_pResults)[i].Targets[0].TargetId.NodeId = ((_ObjectNode_*)p_Node)->BaseAttribute.NodeId;

        MY_TRACE("Returning %s\n", getNodeIdString(&(*a_pResults)[i].Targets[0].TargetId.NodeId));
    }

    *a_pNoOfResults = a_nNoOfBrowsePaths;

    uStatus = response_header_fill(pSession, a_pResponseHeader, a_pRequestHeader, uStatus);
    if (OpcUa_IsBad(uStatus))
    {
        a_pResponseHeader->ServiceResult = OpcUa_BadInternalError;
    }
#ifndef NO_DEBUGGING_
    MY_TRACE("\nSERVICE===END============================================\n\n\n");
#endif /*_DEBUGGING_*/

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

    OpcUa_FinishErrorHandling;
}
