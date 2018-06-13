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
 
/* serverstub (basic includes for implementing a server based on the stack) */
#include <opcua_serverstub.h>
#include <opcua_string.h>
#include <opcua_memory.h>
#include <opcua_trace.h>
#include "addressspace.h"
#include "browseservice.h"
#include "addressspace_init.h"
#include "mytrace.h"
#include "general_header.h"

OPCUA_IMPLEMENT_SCALAR_COMPARE(NodeId, 0)
OPCUA_IMPLEMENT_SCALAR_COPY(NodeId, 0)
OPCUA_IMPLEMENT_ENCODEABLE_COPY(BrowseDescription, 0)

#define MAX_NO_OF_RETURNED_REFERENCES			5

#define Is_my_node(startNodeId,myNode) \
    (OpcUa_IsGood(OpcUa_NodeId_Compare(&(startNodeId),&(myNode).BaseAttribute.NodeId,&result)) && (result==0))

/*============================================================================
 * method which implements the Browse service.
 *===========================================================================*/
OpcUa_StatusCode my_Browse(
    OpcUa_Endpoint                 a_hEndpoint,
    OpcUa_Handle                   a_hContext,
    const OpcUa_RequestHeader*     a_pRequestHeader,
    const OpcUa_ViewDescription*   a_pView,
    OpcUa_UInt32                   a_nRequestedMaxReferencesPerNode,
    OpcUa_Int32                    a_nNoOfNodesToBrowse,
    OpcUa_BrowseDescription*	   a_pNodesToBrowse,
    OpcUa_ResponseHeader*          a_pResponseHeader,
    OpcUa_Int32*                   a_pNoOfResults,
    OpcUa_BrowseResult**           a_pResults,
    OpcUa_Int32*                   a_pNoOfDiagnosticInfos,
    OpcUa_DiagnosticInfo**         a_pDiagnosticInfos)
{
	_BaseAttribute_*		pointer_to_node;
	OpcUa_Int				m;
	extern OpcUa_UInt32		session_flag;
	extern OpcUa_Double		msec_counter;
	extern OpcUa_String*	p_user_name;
	extern OpcUa_UInt32		max_ref_per_node;

    OpcUa_InitializeStatus(OpcUa_Module_Server, "OpcUa_ServerApi_Browse");

    /* validate arguments. */
    OpcUa_ReturnErrorIfArgumentNull(a_hEndpoint);
    OpcUa_ReturnErrorIfArgumentNull(a_hContext);
    OpcUa_ReturnErrorIfArgumentNull(a_pRequestHeader);
    OpcUa_ReturnErrorIfArgumentNull(a_pView);
    OpcUa_ReferenceParameter(a_nRequestedMaxReferencesPerNode);
    OpcUa_ReturnErrorIfArrayArgumentNull(a_nNoOfNodesToBrowse, a_pNodesToBrowse);
    OpcUa_ReturnErrorIfArgumentNull(a_pResponseHeader);
    OpcUa_ReturnErrorIfArrayArgumentNull(a_pNoOfResults, a_pResults);
    OpcUa_ReturnErrorIfArrayArgumentNull(a_pNoOfDiagnosticInfos, a_pDiagnosticInfos);

	*a_pNoOfDiagnosticInfos=0;
	*a_pDiagnosticInfos=OpcUa_Null;
		 
	RESET_SESSION_COUNTER

#ifndef NO_DEBUGGING_
	MY_TRACE("\n\n\nBROWSE SERVICE=============================================\n");
	if(p_user_name!=OpcUa_Null)
		MY_TRACE("\nUser:%s\n",OpcUa_String_GetRawString(p_user_name));  
#endif /*_DEBUGGING_*/


	if(OpcUa_IsBad(session_flag))
	{
		/* Tell client that session is not active. */
#ifndef NO_DEBUGGING_
		MY_TRACE("\nSession not active\n"); 
#endif /*_DEBUGGING_*/
		uStatus=OpcUa_BadSessionNotActivated;
		OpcUa_GotoError;
	}

	
	uStatus=check_authentication_token(a_pRequestHeader);
	if(OpcUa_IsBad(uStatus))
	{
#ifndef NO_DEBUGGING_
		MY_TRACE("\nAuthentication Token ung�ltig.\n"); 
#endif /*_DEBUGGING_*/
		OpcUa_GotoError;
	}

	if(a_nNoOfNodesToBrowse==0)
	{
		uStatus=OpcUa_BadNothingToDo;
		OpcUa_GotoError
	}

	if(a_nRequestedMaxReferencesPerNode>0 && a_nRequestedMaxReferencesPerNode<MAX_NO_OF_RETURNED_REFERENCES)
		max_ref_per_node=a_nRequestedMaxReferencesPerNode;
	else
		max_ref_per_node=MAX_NO_OF_RETURNED_REFERENCES;

	*a_pResults=OpcUa_Memory_Alloc(a_nNoOfNodesToBrowse*sizeof(OpcUa_BrowseResult));
	OpcUa_GotoErrorIfAllocFailed((*a_pResults))
	
	*a_pNoOfResults=a_nNoOfNodesToBrowse;

		for(m=0;m<a_nNoOfNodesToBrowse;m++) /* Check all start nodes */
		{
			OpcUa_BrowseResult_Initialize((*a_pResults+m));
			pointer_to_node= search_for_node((a_pNodesToBrowse+m)->NodeId);
			#ifndef NO_DEBUGGING_
			MY_TRACE("\nBrowse by NodeId:|%d|  NamespaceIndex: |%d|\n",(a_pNodesToBrowse+m)->NodeId.Identifier.Numeric,(a_pNodesToBrowse+m)->NodeId.NamespaceIndex);
			#endif /*_DEBUGGING_*/
			if(pointer_to_node!=OpcUa_Null) /* Browse only existing nodes */
			{
				(*a_pResults+m)->StatusCode=browse((a_pNodesToBrowse+m),(*a_pResults+m),0);
			} /* End: (browse only existing nodes) */
			else
			{
				(*a_pResults+m)->StatusCode=OpcUa_BadNodeIdUnknown; 
			}
			
		} /* End: (check all start nodes) */

	
	uStatus = response_header_fill(a_pResponseHeader,a_pRequestHeader,uStatus);
	if(OpcUa_IsBad(uStatus))
	{
       a_pResponseHeader->ServiceResult=OpcUa_BadInternalError;
	}
#ifndef NO_DEBUGGING_
	MY_TRACE("\nSERVICE===END============================================\n\n\n"); 
#endif /*_DEBUGGING_*/

	RESET_SESSION_COUNTER

    OpcUa_ReturnStatusCode;
    OpcUa_BeginErrorHandling;

   *a_pNoOfResults=0;
	uStatus=response_header_fill(a_pResponseHeader,a_pRequestHeader,uStatus);
	if(OpcUa_IsBad(uStatus))
	{
       a_pResponseHeader->ServiceResult=OpcUa_BadInternalError;
	}
#ifndef NO_DEBUGGING_
	MY_TRACE("\nSERVICE END (WITH ERROR)===========\n\n\n"); 
#endif /*_DEBUGGING_*/
	RESET_SESSION_COUNTER
    OpcUa_FinishErrorHandling;
}

OpcUa_StatusCode browse(OpcUa_BrowseDescription* a_pNodesToBrowse,OpcUa_BrowseResult* a_pResults,OpcUa_Int x)
{
	extern _my_continuationpoint_		Continuation_Point_Data;
	extern OpcUa_Int			Cont_Point_Counter;
	OpcUa_Int					i,n;
	OpcUa_UInt32				NoofRef;
	_BaseAttribute_*			pointer_to_node;
	_BaseAttribute_*			pointer_to_targetnode;
	extern OpcUa_UInt32			max_ref_per_node;
	OpcUa_InitializeStatus(OpcUa_Module_Server, "browse");

	NoofRef						=0;
	pointer_to_node= (_BaseAttribute_*)search_for_node(a_pNodesToBrowse->NodeId);
	OpcUa_ReturnErrorIfNull(pointer_to_node, OpcUa_BadNodeIdUnknown);
#ifndef NO_DEBUGGING_
	MY_TRACE("\nStart Node:%s\n",(pointer_to_node)->DisplayName);
	MY_TRACE("Total number of references to this Node:%d\n",(pointer_to_node)->NoOfReferences);
	if(a_pNodesToBrowse->ReferenceTypeId.Identifier.Numeric!=0 && a_pNodesToBrowse->ReferenceTypeId.IdentifierType==OpcUa_IdentifierType_Numeric)
	{
		_BaseAttribute_* pointer_to_reference_type_node = ((_BaseAttribute_*)search_for_node(a_pNodesToBrowse->ReferenceTypeId));
		OpcUa_ReturnErrorIfNull(pointer_to_reference_type_node, OpcUa_BadNodeIdUnknown);
		MY_TRACE("Browse by Reference: %s\n",(pointer_to_reference_type_node)->DisplayName);
	}
	else
	{
		MY_TRACE("Browse by Reference: Filter criterion not set\n");
	}
#endif /*_DEBUGGING_*/
	if((pointer_to_node)->NoOfReferences)/*if the start node has references ?  Yes->continue with for-loop ,No->nearest start node*/
	{
		for(i=x;i<((pointer_to_node)->NoOfReferences);i++) /* Check all references to start nodes. *******/
		{
			pointer_to_targetnode=(_BaseAttribute_*) search_for_node(((pointer_to_node)->References+i)->Target_NodeId);
			if(pointer_to_targetnode!=OpcUa_Null)
			{
				if(/* Check filter masks: ReferencesTypeId, NodeClassMask*/ is_subnode(a_pNodesToBrowse->ReferenceTypeId,(pointer_to_node->References+i)->ReferenceTypeId,a_pNodesToBrowse->IncludeSubtypes)==OpcUa_True && check_Mask(a_pNodesToBrowse->NodeClassMask, pointer_to_targetnode->NodeClass)==OpcUa_True)
				{	
					if(/* Check filter mask browsedir.*/check_dir(a_pNodesToBrowse->BrowseDirection,(pointer_to_node->References+i))==OpcUa_True)
					{
									#ifndef NO_DEBUGGING_
										MY_TRACE("TargetNode returned: %s",pointer_to_targetnode->DisplayName);
									#endif /*_DEBUGGING_*/
									a_pResults->References=OpcUa_Memory_ReAlloc(a_pResults->References,(NoofRef+1)*sizeof(OpcUa_ReferenceDescription));
									OpcUa_GotoErrorIfAllocFailed((a_pResults->References))
									OpcUa_ReferenceDescription_Initialize((a_pResults ->References+NoofRef));
	
									/*NodeId of ReferenceType*/
									if(check_Mask(a_pNodesToBrowse->ResultMask,OpcUa_BrowseResultMask_ReferenceTypeId)== OpcUa_True)/*if False is masked*/
									{
										(a_pResults ->References+NoofRef)->ReferenceTypeId=(pointer_to_node->References+i)->ReferenceTypeId;
									}
									/*************************/
									
									/*IsForward criteria*/
									if(check_Mask(a_pNodesToBrowse->ResultMask,OpcUa_BrowseResultMask_IsForward)== OpcUa_True)/*if False is masked*/
									{
										if(((pointer_to_node)->References+i)->IsInverse==OpcUa_True)
										{
											(a_pResults ->References+NoofRef)->IsForward=OpcUa_False;
										}
										else
										{
											(a_pResults ->References+NoofRef)->IsForward=OpcUa_True;
										}
									}
									/*************************/
	
									/*NodeId of target Node*/
									uStatus=OpcUa_NodeId_CopyTo(&pointer_to_targetnode->NodeId,&(a_pResults->References+NoofRef)->NodeId.NodeId);
									OpcUa_GotoErrorIfBad(uStatus);
									(a_pResults ->References+NoofRef)->NodeId.ServerIndex=0;
									#ifndef NO_DEBUGGING_
										MY_TRACE("|%d| |%d|\n",pointer_to_targetnode->NodeId.NamespaceIndex,pointer_to_targetnode->NodeId.Identifier.Numeric);
									#endif /*_DEBUGGING_*/
									/**********************/
	
									/*BrowseName of target Node*/
									if(check_Mask(a_pNodesToBrowse->ResultMask,OpcUa_BrowseResultMask_BrowseName)== OpcUa_True)/*if False is masked*/
									{
										OpcUa_String_AttachCopy(&((a_pResults ->References+NoofRef)->BrowseName.Name),pointer_to_targetnode->BrowseName);
										(a_pResults ->References+NoofRef)->BrowseName.NamespaceIndex=pointer_to_targetnode->NodeId.NamespaceIndex;
									}
									/***************************/
	
									/*DisplayName of target Node*/
									if(check_Mask(a_pNodesToBrowse->ResultMask,OpcUa_BrowseResultMask_DisplayName)== OpcUa_True)/*if False is masked*/
									{
										OpcUa_String_AttachCopy(&(a_pResults ->References+NoofRef)->DisplayName.Text,pointer_to_targetnode->DisplayName);
										OpcUa_String_AttachCopy(&(a_pResults ->References+NoofRef)->DisplayName.Locale,"");
									}
									/***************************/
	
									/*NodeClass of target Node*/
									if(check_Mask(a_pNodesToBrowse->ResultMask,OpcUa_BrowseResultMask_NodeClass)== OpcUa_True)/*if False is masked*/
									{
										(a_pResults ->References+NoofRef)->NodeClass=pointer_to_targetnode->NodeClass;
									}
									/***************************/
	
									/*TypeDefinition of target Node*/
									if(check_Mask(a_pNodesToBrowse->ResultMask,OpcUa_BrowseResultMask_TypeDefinition)== OpcUa_True)/*if False is masked*/
									{
										if((pointer_to_targetnode->NodeClass)==OpcUa_NodeClass_Object ||(pointer_to_targetnode->NodeClass)==OpcUa_NodeClass_Variable )
										{
											for(n=0;n<(pointer_to_targetnode->NoOfReferences);n++)
											{
												if(((pointer_to_targetnode->References+n)->ReferenceTypeId.Identifier.Numeric)==OpcUaId_HasTypeDefinition)
												{
													uStatus=OpcUa_NodeId_CopyTo(&(pointer_to_targetnode->References+n)->Target_NodeId,&(a_pResults->References + NoofRef)->TypeDefinition.NodeId);
													OpcUa_GotoErrorIfBad(uStatus);
													(a_pResults ->References+NoofRef)->TypeDefinition.ServerIndex=0;
												}
											}
										}
									}
									NoofRef++;
									/*******************************/
									if(NoofRef >= max_ref_per_node)
									{
										if(need_continuationpoint(a_pNodesToBrowse,(i+1))==OpcUa_True )
										{
											/* Get the next continuation point ID. */
											OpcUa_GotoErrorIfTrue(Continuation_Point_Data.Cont_Point_Identifier!=0,OpcUa_BadNoContinuationPoints);
											Cont_Point_Counter++;
											if(Cont_Point_Counter==0)
											{
												/* Handle rollover. Avoid 0 which means None. */
												Cont_Point_Counter++;
											}

											/* Return the counter value as the ContinuationPoint. */
											a_pResults->ContinuationPoint.Data=(OpcUa_Byte*)OpcUa_Memory_Alloc(sizeof(OpcUa_Int));
											OpcUa_GotoErrorIfAllocFailed((a_pResults->ContinuationPoint.Data))
											a_pResults->ContinuationPoint.Length=sizeof(OpcUa_Int);
											*((OpcUa_Int*)a_pResults->ContinuationPoint.Data)=Cont_Point_Counter;

											OpcUa_BrowseDescription_Clear(&Continuation_Point_Data.NodeToBrowse);
											uStatus=OpcUa_BrowseDescription_CopyTo(a_pNodesToBrowse,&Continuation_Point_Data.NodeToBrowse);
											OpcUa_GotoErrorIfBad(uStatus);
											Continuation_Point_Data.Current_Ref=(i+1);
											Continuation_Point_Data.Cont_Point_Identifier=Cont_Point_Counter;
											#ifndef NO_DEBUGGING_
											{
												_BaseAttribute_* pointer_to_reference_node=((_BaseAttribute_*)search_for_node(((pointer_to_node)->References+(i+1))->Target_NodeId));
												OpcUa_GotoErrorIfNull(pointer_to_reference_node,OpcUa_BadNodeIdUnknown);
												MY_TRACE("\nContinuationPoint (Identifier:%d) set for this Start Node.\n",Continuation_Point_Data.Cont_Point_Identifier);
												MY_TRACE("and points to the next TargetNode:%s\n",(pointer_to_reference_node)->DisplayName);
											}
											#endif /*_DEBUGGING_*/
											break;
										}
									}
					} /* End: filter mask browsedir. */	
				} /* End: filter masks ReferencesTypeId, NodeClassMask. */
			}
			
		} /* End: (Check all references to start nodes)  *********************************************************/
		a_pResults ->NoOfReferences=NoofRef;
	}
	else
	{
		a_pResults ->NoOfReferences=0;
		a_pResults->References=OpcUa_Null; /* Start node has no references. Continue with next start node. */
		#ifndef NO_DEBUGGING_
			MY_TRACE("\nStart Node has no references\n");
		#endif /*_DEBUGGING_*/
	}

	OpcUa_ReturnStatusCode;
	OpcUa_BeginErrorHandling;
	
		if((a_pResults->References)!=OpcUa_Null)
		{
			OpcUa_ReferenceDescription_Clear(a_pResults->References);
			a_pResults ->NoOfReferences=0;
		}
		uStatus=OpcUa_BadOutOfMemory;
	
	OpcUa_FinishErrorHandling;
}

OpcUa_Void* search_for_node(OpcUa_NodeId NodeId)
{
	OpcUa_Void* p_Node=OpcUa_Null;
	OpcUa_Int i;
	OpcUa_Int32 result;

		//Check all ObjectTypeNodes--------------------------------------------------------
		
			for(i=0;i<ARRAY_SIZE_(all_ObjectTypeNodes);i++)
			{
				if(Is_my_node(NodeId,all_ObjectTypeNodes[i]))
				{
					p_Node=(all_ObjectTypeNodes+i);
					break;
				}
				
			}
	
		
	   
		//Check all ObjectNodes--------------------------------------------------------
		for(i=0;i<ARRAY_SIZE_(all_ObjectNodes);i++)
			{
				if(Is_my_node(NodeId,all_ObjectNodes[i]))
				{
					p_Node=(all_ObjectNodes+i);
					break;
				}
				
			}

		//Check all ReferenceTypeNodes--------------------------------------------------------
		for(i=0;i<ARRAY_SIZE_(all_ReferencesTypeNodes);i++)
			{
				if(Is_my_node(NodeId,all_ReferencesTypeNodes[i]))
				{
					p_Node=(all_ReferencesTypeNodes+i);
					break;
				}
				
			}

		//Check all VariableNodes--------------------------------------------------------
			for(i=0;i<ARRAY_SIZE_(all_VariableNodes);i++)
			{
				if(Is_my_node(NodeId,all_VariableNodes[i]))
				{
					p_Node=(all_VariableNodes+i);
					break;
				}
				
			}

		//Check all VariableTypeNodes--------------------------------------------------------
		for(i=0;i<ARRAY_SIZE_(all_VariableTypeNodes);i++)
			{
				if(Is_my_node(NodeId,all_VariableTypeNodes[i]))
				{
					p_Node=(all_VariableTypeNodes+i);
					break;
				}
				
			}

		//Check all DataTypeNodes--------------------------------------------------------
		for(i=0;i<ARRAY_SIZE_(all_DataTypeNodes);i++)
			{
				if(Is_my_node(NodeId,all_DataTypeNodes[i]))
				{
					p_Node=(all_DataTypeNodes+i);
					break;
				}
				
			}
	

	return p_Node;
}


OpcUa_Boolean  is_subnode( OpcUa_NodeId  start_NodeId, OpcUa_NodeId  desired_node,OpcUa_Boolean IncludeSubtypes)
{
	
		OpcUa_Int z;
		_BaseAttribute_* p_Node;
		_BaseAttribute_* p_Node_2;
		OpcUa_StatusCode uStatus;
		OpcUa_Int32 result;
		
		if(start_NodeId.Identifier.Numeric==0 && start_NodeId.IdentifierType==OpcUa_IdentifierType_Numeric)
		{
			return OpcUa_True;
		}
		uStatus=OpcUa_NodeId_Compare(&start_NodeId,&desired_node,&result);
		
		if(OpcUa_IsGood(uStatus) && (result==0))
		{ 
			return OpcUa_True;
		}
		else
		{
			if (IncludeSubtypes==OpcUa_False)
			{
				return OpcUa_False;
			}
			if((p_Node=(_BaseAttribute_*)search_for_node(start_NodeId))!=OpcUa_Null)
			{
				for(z=0;z<(p_Node->NoOfReferences);z++)
				{
					uStatus=OpcUa_NodeId_Compare(&(p_Node->References+z)->Target_NodeId,&desired_node,&result);
					if(OpcUa_IsGood(uStatus) && (result==0))
					{
						return OpcUa_True;
					}
					else
					{
						p_Node_2=(_BaseAttribute_*)search_for_node((p_Node->References+z)->Target_NodeId);
						if((p_Node_2->NoOfReferences)!=0 )
						{
							if(is_subnode((p_Node->References+z)->Target_NodeId,desired_node,OpcUa_True)==OpcUa_True)
								return OpcUa_True;
						}
					}
				}
			}
		}
	
	return OpcUa_False;
}


OpcUa_Boolean check_Mask(OpcUa_UInt32 Mask,OpcUa_UInt32 attribute_of_targetNode_or_RefId)
{
	if(Mask>0)
	{
		if((Mask & attribute_of_targetNode_or_RefId)>0)
			return OpcUa_True;
		return OpcUa_False;
	}
	return OpcUa_True;
}

OpcUa_Boolean check_dir(OpcUa_BrowseDirection browsedir,_ReferenceNode_* p_ref)
{
	if(browsedir==OpcUa_BrowseDirection_Both)
		return OpcUa_True;
	else
	{
		if(browsedir==OpcUa_BrowseDirection_Forward && p_ref->IsInverse==OpcUa_False)
			return OpcUa_True;
		if(browsedir==OpcUa_BrowseDirection_Inverse && p_ref->IsInverse==OpcUa_True)
			return OpcUa_True;
	}
	return OpcUa_False;
}



OpcUa_Boolean need_continuationpoint(OpcUa_BrowseDescription* NodeToBrowse,OpcUa_Int x)
{
	OpcUa_Int i,counter;
	_BaseAttribute_*		pointer_to_node;
	_BaseAttribute_*		pointer_to_targetnode;

	counter=0;

	pointer_to_node= (_BaseAttribute_*)search_for_node(NodeToBrowse->NodeId);

	for(i=x;i<((pointer_to_node)->NoOfReferences);i++)/* Iterate over all references of start node. */
	{
		pointer_to_targetnode=(_BaseAttribute_*) search_for_node(((pointer_to_node)->References+i)->Target_NodeId);
		if( is_subnode(NodeToBrowse->ReferenceTypeId,(pointer_to_node->References+i)->ReferenceTypeId,NodeToBrowse->IncludeSubtypes)==OpcUa_True && check_Mask(NodeToBrowse->NodeClassMask, pointer_to_targetnode->NodeClass))
		{	
			if(check_dir(NodeToBrowse->BrowseDirection,(pointer_to_node->References+i))==OpcUa_True)
			{
				counter++;				
			}
		}
	}
	if(counter>0)
		return OpcUa_True;   /* Continuationpoint is needed. */
	else 
		return OpcUa_False;  /* Do not need continuationpoint. */
}
