from typing import List
from fastapi import HTTPException, status
from datetime import datetime

from app.schemas.conversation import ConversationResponse, PaginatedConversationResponse
from app.db.cassandra import CassandraClient

class ConversationController:
    """
    Controller for handling conversation operations
    This is a stub that students will implement
    """
    
    def __init__(self):
        self.db = CassandraClient()
    
    async def get_user_conversations(
        self, 
        user_id: int, 
        page: int = 1, 
        limit: int = 20
    ) -> PaginatedConversationResponse:
        """
        Get all conversations for a user with pagination
        
        Args:
            user_id: ID of the user
            page: Page number
            limit: Number of conversations per page
            
        Returns:
            Paginated list of conversations
            
        Raises:
            HTTPException: If user not found or access denied
        """
        try:
            # Get total count first
            count_query = """
                SELECT COUNT(*) as count
                FROM user_conversations
                WHERE user_id = %s
            """
            count_result = self.db.execute(count_query, (user_id,))
            total = count_result[0].count if count_result else 0
            
            # Get paginated conversations
            query = """
                SELECT conversation_id, last_message_timestamp, participant_ids
                FROM user_conversations
                WHERE user_id = %s
                LIMIT %s
            """
            offset = (page - 1) * limit
            results = self.db.execute(query, (user_id, limit))
            
            conversations = []
            for row in results:
                # Get the other user's ID from participant_ids
                other_user_id = next(uid for uid in row.participant_ids if uid != user_id)
                
                # Get last message
                last_message_query = """
                    SELECT message_text
                    FROM messages_by_conversation
                    WHERE conversation_id = %s
                    LIMIT 1
                """
                last_message_result = self.db.execute(last_message_query, (row.conversation_id,))
                last_message = last_message_result[0].message_text if last_message_result else None
                
                conversations.append(ConversationResponse(
                    id=row.conversation_id,
                    user1_id=user_id,
                    user2_id=other_user_id,
                    last_message_at=row.last_message_timestamp,
                    last_message_content=last_message
                ))
            
            return PaginatedConversationResponse(
                total=total,
                page=page,
                limit=limit,
                data=conversations
            )
            
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to fetch conversations: {str(e)}"
            )
    
    async def get_conversation(self, conversation_id: int) -> ConversationResponse:
        """
        Get a specific conversation by ID
        
        Args:
            conversation_id: ID of the conversation
            
        Returns:
            Conversation details
            
        Raises:
            HTTPException: If conversation not found or access denied
        """
        try:
            # Get conversation details
            query = """
                SELECT participant_ids, created_at
                FROM conversations
                WHERE conversation_id = %s
            """
            result = self.db.execute(query, (conversation_id,))
            
            if not result:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Conversation not found"
                )
            
            row = result[0]
            participant_ids = list(row.participant_ids)
            
            # Get last message
            last_message_query = """
                SELECT message_text, created_at
                FROM messages_by_conversation
                WHERE conversation_id = %s
                LIMIT 1
            """
            last_message_result = self.db.execute(last_message_query, (conversation_id,))
            
            last_message = None
            last_message_at = row.created_at
            if last_message_result:
                last_message = last_message_result[0].message_text
                last_message_at = last_message_result[0].created_at
            
            return ConversationResponse(
                id=conversation_id,
                user1_id=participant_ids[0],
                user2_id=participant_ids[1],
                last_message_at=last_message_at,
                last_message_content=last_message
            )
            
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to fetch conversation: {str(e)}"
            ) 