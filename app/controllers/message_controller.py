from typing import Optional, List
from datetime import datetime
from uuid import uuid4
from fastapi import HTTPException, status

from app.schemas.message import MessageCreate, MessageResponse, PaginatedMessageResponse
from app.db.cassandra import CassandraClient

class MessageController:
    """
    Controller for handling message operations
    This is a stub that students will implement
    """
    
    def __init__(self):
        self.db = CassandraClient()
    
    async def send_message(self, message_data: MessageCreate) -> MessageResponse:
        """
        Send a message from one user to another
        
        Args:
            message_data: The message data including content, sender_id, and receiver_id
            
        Returns:
            The created message with metadata
        
        Raises:
            HTTPException: If message sending fails
        """
        try:
            # Generate UUIDs for message and conversation
            message_id = uuid4()
            conversation_id = None
            
            # Check if conversation exists between these users
            query = """
                SELECT conversation_id 
                FROM user_conversations 
                WHERE user_id = %s 
                AND participant_ids CONTAINS %s 
                LIMIT 1
            """
            result = self.db.execute(query, (message_data.sender_id, message_data.receiver_id))
            
            if result:
                conversation_id = result[0].conversation_id
            else:
                # Create new conversation
                conversation_id = uuid4()
                participant_ids = {message_data.sender_id, message_data.receiver_id}
                
                # Insert into conversations table
                query = """
                    INSERT INTO conversations (conversation_id, participant_ids, created_at)
                    VALUES (%s, %s, %s)
                """
                self.db.execute(query, (conversation_id, participant_ids, datetime.utcnow()))
                
                # Insert into user_conversations for both users
                query = """
                    INSERT INTO user_conversations (user_id, last_message_timestamp, conversation_id, participant_ids)
                    VALUES (%s, %s, %s, %s)
                """
                timestamp = datetime.utcnow()
                self.db.execute(query, (message_data.sender_id, timestamp, conversation_id, participant_ids))
                self.db.execute(query, (message_data.receiver_id, timestamp, conversation_id, participant_ids))
            
            # Insert message
            timestamp = datetime.utcnow()
            query = """
                INSERT INTO messages_by_conversation 
                (conversation_id, created_at, message_id, sender_id, message_text)
                VALUES (%s, %s, %s, %s, %s)
            """
            self.db.execute(query, (
                conversation_id,
                timestamp,
                message_id,
                message_data.sender_id,
                message_data.content
            ))
            
            # Update conversation last activity
            query = """
                UPDATE user_conversations 
                SET last_message_timestamp = %s
                WHERE user_id IN (%s, %s) AND conversation_id = %s
            """
            self.db.execute(query, (
                timestamp,
                message_data.sender_id,
                message_data.receiver_id,
                conversation_id
            ))
            
            return MessageResponse(
                id=message_id,
                content=message_data.content,
                sender_id=message_data.sender_id,
                receiver_id=message_data.receiver_id,
                created_at=timestamp,
                conversation_id=conversation_id
            )
            
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to send message: {str(e)}"
            )
    
    async def get_conversation_messages(
        self, 
        conversation_id: int, 
        page: int = 1, 
        limit: int = 20
    ) -> PaginatedMessageResponse:
        """
        Get all messages in a conversation with pagination
        
        Args:
            conversation_id: ID of the conversation
            page: Page number
            limit: Number of messages per page
            
        Returns:
            Paginated list of messages
            
        Raises:
            HTTPException: If conversation not found or access denied
        """
        try:
            # Get total count first
            count_query = """
                SELECT COUNT(*) as count
                FROM messages_by_conversation
                WHERE conversation_id = %s
            """
            count_result = self.db.execute(count_query, (conversation_id,))
            total = count_result[0].count if count_result else 0
            
            # Get paginated messages
            query = """
                SELECT message_id, conversation_id, created_at, sender_id, message_text
                FROM messages_by_conversation
                WHERE conversation_id = %s
                LIMIT %s
            """
            offset = (page - 1) * limit
            results = self.db.execute(query, (conversation_id, limit))
            
            messages = []
            for row in results:
                messages.append(MessageResponse(
                    id=row.message_id,
                    content=row.message_text,
                    sender_id=row.sender_id,
                    conversation_id=row.conversation_id,
                    created_at=row.created_at,
                    receiver_id=None  # We don't store receiver_id in messages table
                ))
            
            return PaginatedMessageResponse(
                total=total,
                page=page,
                limit=limit,
                data=messages
            )
            
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to fetch messages: {str(e)}"
            )
    
    async def get_messages_before_timestamp(
        self, 
        conversation_id: int, 
        before_timestamp: datetime,
        page: int = 1, 
        limit: int = 20
    ) -> PaginatedMessageResponse:
        """
        Get messages in a conversation before a specific timestamp with pagination
        
        Args:
            conversation_id: ID of the conversation
            before_timestamp: Get messages before this timestamp
            page: Page number
            limit: Number of messages per page
            
        Returns:
            Paginated list of messages
            
        Raises:
            HTTPException: If conversation not found or access denied
        """
        try:
            # Get total count of messages before timestamp
            count_query = """
                SELECT COUNT(*) as count
                FROM messages_by_conversation
                WHERE conversation_id = %s
                AND created_at < %s
            """
            count_result = self.db.execute(count_query, (conversation_id, before_timestamp))
            total = count_result[0].count if count_result else 0
            
            # Get paginated messages before timestamp
            query = """
                SELECT message_id, conversation_id, created_at, sender_id, message_text
                FROM messages_by_conversation
                WHERE conversation_id = %s
                AND created_at < %s
                LIMIT %s
            """
            offset = (page - 1) * limit
            results = self.db.execute(query, (conversation_id, before_timestamp, limit))
            
            messages = []
            for row in results:
                messages.append(MessageResponse(
                    id=row.message_id,
                    content=row.message_text,
                    sender_id=row.sender_id,
                    conversation_id=row.conversation_id,
                    created_at=row.created_at,
                    receiver_id=None  # We don't store receiver_id in messages table
                ))
            
            return PaginatedMessageResponse(
                total=total,
                page=page,
                limit=limit,
                data=messages
            )
            
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to fetch messages: {str(e)}"
            ) 