'use client';

/**
 * Real-time hooks powered by kalam-link SDK subscriptions
 * 
 * These hooks use WebSocket subscriptions (via kalam-link) for real-time
 * message and typing indicator updates, replacing HTTP polling.
 * 
 * Architecture:
 * - Messages: subscribeWithSql("SELECT * FROM chat.messages WHERE conversation_id = X")
 * - Typing: subscribeWithSql("SELECT * FROM chat.typing_indicators WHERE ... AND is_typing = true")
 * - Conversations: queryAll + subscribe for live updates
 */

import { useState, useEffect, useCallback, useRef } from 'react';
import { useKalamDB } from '@/providers/kalamdb-provider';
import { KALAMDB_CONFIG } from '@/lib/config';
import { parseTimestamp } from '@/lib/utils';
import type { Conversation, Message, TypingIndicator, ConnectionStatus, FileRef, FileAttachment, AiTypingStatus } from '@/types';
import type { ServerMessage, Unsubscribe, UploadProgress, RowData } from 'kalam-link';

/** Map a `RowData` query row to a `Conversation` using KalamCellValue accessors. */
function rowToConversation(row: RowData): Conversation {
  return {
    id: row['id']?.asString() ?? '',
    title: row['title']?.asString() ?? '',
    created_by: row['created_by']?.asString() ?? '',
    created_at: row['created_at']?.asString() ?? '',
    updated_at: row['updated_at']?.asString() ?? '',
  };
}

/** Map a `RowData` query row to a `Message` using KalamCellValue accessors. */
function rowToMessage(row: RowData): Message {
  const fileRaw = row['file_data']?.asObject() ?? (row['file_data']?.asString() ? row['file_data'].asString() : undefined);
  const file = parseFileRef(fileRaw as string | FileRef | undefined);
  return {
    id: row['id']?.asString() ?? '',
    client_id: row['client_id']?.asString() ?? undefined,
    conversation_id: row['conversation_id']?.asString() ?? '',
    sender: row['sender']?.asString() ?? '',
    role: (row['role']?.asString() ?? 'user') as Message['role'],
    content: row['content']?.asString() ?? '',
    status: (row['status']?.asString() ?? 'sent') as Message['status'],
    created_at: row['created_at']?.asString() ?? '',
    files: file ? [fileRefToAttachment(file)] : undefined,
  };
}

// ============================================================================
// useConversations - Fetch and live-subscribe to conversations
// ============================================================================

export function useConversations() {
  const { client, isReady } = useKalamDB();
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const unsubRef = useRef<Unsubscribe | null>(null);

  // Subscribe to conversations table for real-time updates
  useEffect(() => {
    console.log('[useConversations] Effect triggered, client:', !!client, 'isReady:', isReady);
    if (!client || !isReady) {
      console.log('[useConversations] Waiting for client/ready...');
      return;
    }

    let cancelled = false;

    const setupSubscription = async () => {
      try {
        console.log('[useConversations] Setting up subscription...');
        setError(null);

        // Subscribe with SQL - gets initial data + live changes
        console.log('[useConversations] Calling subscribeWithSql...');
        const unsub = await client.subscribeWithSql(
          'SELECT * FROM chat.conversations ORDER BY updated_at DESC',
          (event: ServerMessage) => {
            if (cancelled) return;
            console.log('[useConversations] Subscription event:', event.type, event);

            if (event.type === 'initial_data_batch' || event.type === 'subscription_ack') {
              // Initial data load
              if ('rows' in event && event.rows) {
                console.log('[useConversations] Received rows:', event.rows.length);
                setConversations(prev => {
                  const rows = event.rows as unknown as Conversation[];
                  const merged = mergeRows<Conversation>(prev, rows);
                  return merged;
                });
              }
              if ('batch_control' in event && event.batch_control?.status === 'ready') {
                console.log('[useConversations] Subscription ready');
                setLoading(false);
              }
            } else if (event.type === 'change') {
              if ('rows' in event && event.rows) {
                const rows = event.rows as unknown as Conversation[];
                console.log('[useConversations] Change event:', 'change_type' in event ? event.change_type : 'unknown', 'rows:', rows.length);
                if ('change_type' in event) {
                  if (event.change_type === 'insert') {
                    setConversations(prev => [...rows, ...prev]);
                  } else if (event.change_type === 'update') {
                    setConversations(prev =>
                      prev.map(c => {
                        const updated = rows.find(r => r.id === c.id);
                        return updated || c;
                      })
                    );
                  } else if (event.change_type === 'delete') {
                    const deletedIds = new Set(rows.map(r => r.id));
                    setConversations(prev => prev.filter(c => !deletedIds.has(c.id)));
                  }
                }
              }
            }
          },
          { batch_size: 100 }
        );

        console.log('[useConversations] Subscription created successfully');
        unsubRef.current = unsub;
      } catch (err) {
        if (!cancelled) {
          console.error('[useConversations] Subscription error:', err);
          console.error('[useConversations] Error details:', err instanceof Error ? err.stack : err);
          setError(err instanceof Error ? err.message : 'Subscription failed');
          setLoading(false);
          // Fallback: try query-based fetch
          console.log('[useConversations] Falling back to query...');
          await fetchConversationsViaQuery();
        }
      }
    };

    const fetchConversationsViaQuery = async () => {
      try {
        const rows = await client.queryAll(
          'SELECT * FROM chat.conversations ORDER BY updated_at DESC'
        );
        if (!cancelled) {
          setConversations(rows.map(rowToConversation));
          setLoading(false);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : 'Failed to fetch conversations');
          setLoading(false);
        }
      }
    };

    setupSubscription();

    return () => {
      cancelled = true;
      if (unsubRef.current) {
        unsubRef.current().catch(() => {});
        unsubRef.current = null;
      }
    };
  }, [client, isReady]);

  const createConversation = useCallback(async (title: string): Promise<Conversation | null> => {
    if (!client) return null;

    try {
      const escapedTitle = title.replace(/'/g, "''");
      await client.query(
        `INSERT INTO chat.conversations (title, created_by) VALUES ('${escapedTitle}', 'admin')`
      );

      // The subscription will pick up the new conversation automatically
      // But fetch immediately for optimistic UI
      const result = await client.queryAll(
        `SELECT * FROM chat.conversations WHERE title = '${escapedTitle}' ORDER BY created_at DESC LIMIT 1`
      );
      
      const newConv = result.length > 0 ? rowToConversation(result[0]) : null;
      if (newConv) {
        setConversations(prev => {
          // Avoid duplicates (subscription might have already added it)
          if (prev.some(c => c.id === newConv.id)) return prev;
          return [newConv, ...prev];
        });
      }
      return newConv;
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to create conversation');
      return null;
    }
  }, [client]);

  const deleteConversation = useCallback(async (conversationId: string): Promise<boolean> => {
    if (!client) return false;

    try {
      // Delete messages first (cascade)
      await client.query(
        `DELETE FROM chat.messages WHERE conversation_id = ${conversationId}`
      );
      
      // Note: typing_indicators is a stream table (append-only), so we can't DELETE from it
      // Old records will naturally age out or be filtered by queries
      
      // Delete conversation
      await client.query(
        `DELETE FROM chat.conversations WHERE id = ${conversationId}`
      );
      
      // Update local state
      setConversations(prev => prev.filter(c => c.id !== conversationId));
      return true;
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to delete conversation');
      return false;
    }
  }, [client]);

  const refetch = useCallback(async () => {
    if (!client) return;
    try {
      const convs = await client.queryAll(
        'SELECT * FROM chat.conversations ORDER BY updated_at DESC'
      );
      setConversations(convs.map(rowToConversation));
    } catch (err) {
      console.error('[useConversations] Refetch error:', err);
    }
  }, [client]);

  return { conversations, loading, error, createConversation, deleteConversation, refetch };
}

// ============================================================================
// useMessages - Subscribe to messages for a conversation (real-time)
// ============================================================================

export function useMessages(conversationId: string | null) {
  const { client, isReady } = useKalamDB();
  const [messages, setMessages] = useState<Message[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [sending, setSending] = useState(false);
  const [uploadProgress, setUploadProgress] = useState<UploadProgress | null>(null);
  const [waitingForAI, setWaitingForAI] = useState(false);
  const unsubRef = useRef<Unsubscribe | null>(null);
  const lastUserMessageTimeRef = useRef<number>(0);
  const waitingTimeoutRef = useRef<NodeJS.Timeout | null>(null);

  // Timeout for waiting state (30 seconds)
  useEffect(() => {
    if (waitingForAI) {
      waitingTimeoutRef.current = setTimeout(() => {
        console.log('[useMessages] AI response timeout, clearing waiting state');
        setWaitingForAI(false);
      }, 30000);
      
      return () => {
        if (waitingTimeoutRef.current) {
          clearTimeout(waitingTimeoutRef.current);
          waitingTimeoutRef.current = null;
        }
      };
    }
  }, [waitingForAI]);

  // Subscribe to messages for current conversation
  useEffect(() => {
    console.log('[useMessages] Effect triggered, conversationId:', conversationId, 'client:', !!client, 'isReady:', isReady);
    if (!client || !isReady || !conversationId) {
      console.log('[useMessages] Not ready or no conversation selected');
      setMessages([]);
      setLoading(false);
      setWaitingForAI(false); // Clear waiting state when changing conversations
      return;
    }

    let cancelled = false;
    setLoading(true);
    setMessages([]);

    const setupSubscription = async () => {
      // Clean up previous subscription
      if (unsubRef.current) {
        console.log('[useMessages] Cleaning up previous subscription');
        await unsubRef.current().catch(() => {});
        unsubRef.current = null;
      }

      try {
        console.log('[useMessages] Setting up subscription for conversation:', conversationId);
        setError(null);

        const sql = `SELECT * FROM chat.messages WHERE conversation_id = ${conversationId} ORDER BY created_at ASC`;
        console.log('[useMessages] SQL:', sql);
        const unsub = await client.subscribeWithSql(
          sql,
          (event: ServerMessage) => {
            if (cancelled) return;
            console.log('[useMessages] Subscription event:', event.type, event);

            if (event.type === 'initial_data_batch') {
              if ('rows' in event && event.rows) {
                console.log('[useMessages] Received initial batch:', event.rows.length, 'messages');
                setMessages(prev => mergeMessageRows(prev, event.rows as unknown as RawMessageRow[]));
              }
              if ('batch_control' in event && event.batch_control?.status === 'ready') {
                console.log('[useMessages] Subscription ready');
                setLoading(false);
              }
            } else if (event.type === 'subscription_ack') {
              // Initial ack - loading started  
              console.log('[useMessages] Subscription acknowledged');
              if ('batch_control' in event && event.batch_control?.status === 'ready') {
                setLoading(false);
              }
            } else if (event.type === 'change') {
              if ('rows' in event && event.rows) {
                const rows = event.rows as unknown as RawMessageRow[];
                console.log('[useMessages] Change event:', 'change_type' in event ? event.change_type : 'unknown', 'rows:', rows.length);
                if ('change_type' in event) {
                  if (event.change_type === 'insert' || event.change_type === 'update') {
                    setMessages(prev => mergeMessageRows(prev, rows));
                    // Check if AI responded - clear waiting state
                    const hasNewAssistantMessage = rows.some(r => r.role === 'assistant');
                    if (hasNewAssistantMessage) {
                      console.log('[useMessages] AI response received, clearing waiting state');
                      setWaitingForAI(false);
                      if (waitingTimeoutRef.current) {
                        clearTimeout(waitingTimeoutRef.current);
                        waitingTimeoutRef.current = null;
                      }
                    }
                  } else if (event.change_type === 'delete') {
                    setMessages(prev => removeMessageRows(prev, rows));
                  }
                }
              }
            }
          },
          { batch_size: 200 }
        );

        console.log('[useMessages] Subscription created successfully');
        unsubRef.current = unsub;
      } catch (err) {
        if (!cancelled) {
          console.error('[useMessages] Subscription error, falling back to query:', err);
          setError(err instanceof Error ? err.message : 'Subscription failed');
          // Fallback: query-based fetch
          await fetchMessagesViaQuery();
        }
      }
    };

    const fetchMessagesViaQuery = async () => {
      try {
        const msgs = await client.queryAll(
          `SELECT * FROM chat.messages WHERE conversation_id = ${conversationId} ORDER BY created_at ASC`
        );
        if (!cancelled) {
          setMessages(mergeMessageRows([], msgs.map(rowToMessage) as RawMessageRow[]));
          setLoading(false);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : 'Failed to fetch messages');
          setLoading(false);
        }
      }
    };

    setupSubscription();

    return () => {
      cancelled = true;
      if (unsubRef.current) {
        unsubRef.current().catch(() => {});
        unsubRef.current = null;
      }
    };
  }, [client, isReady, conversationId]);

  const sendMessage = useCallback(async (content: string, files?: File[]): Promise<Message | null> => {
    if (!client || !conversationId || (!content.trim() && (!files || files.length === 0))) return null;

    const trimmed = content.trim();
    const clientId = createClientId();
    console.log('[useMessages] Sending message:', trimmed.substring(0, 50), 'with', files?.length || 0, 'files');
    setSending(true);
    setUploadProgress(null);
    try {
      // Optimistic update
      const optimisticMsg: Message = {
        id: `temp-${Date.now()}`,
        client_id: clientId,
        conversation_id: conversationId,
        sender: 'admin',
        role: 'user',
        content: trimmed,
        status: 'sending',
        created_at: new Date().toISOString(),
        files: files && files.length > 0 ? createOptimisticFiles(files) : undefined,
      };
      setMessages(prev => mergeMessageRows(prev, [optimisticMsg as RawMessageRow]));

      const escapedContent = trimmed.replace(/'/g, "''");
      const safeContent = escapedContent.length > 0 ? escapedContent : '';

      if (files && files.length > 0) {
        console.log('[useMessages] Uploading', files.length, 'files');
        const placeholders = files.map((file, index) => {
          const fileKey = `file_${index}`;
          const fileLiteral = `FILE(\"${fileKey}\")`;
          return `('${clientId}', ${conversationId}, 'admin', 'user', '${safeContent}', ${fileLiteral}, 'sent')`;
        });

        const filesMap = files.reduce<Record<string, File>>((acc, file, index) => {
          acc[`file_${index}`] = file;
          return acc;
        }, {});

        const insertSql = `INSERT INTO chat.messages (client_id, conversation_id, sender, role, content, file_data, status) VALUES ${placeholders.join(', ')}`;
        console.log('[useMessages] INSERT SQL (files):', insertSql);
        await client.queryWithFiles(
          insertSql,
          filesMap,
          undefined,
          (progress) => {
            const fileName = files[progress.file_index - 1]?.name;
            setUploadProgress({
              ...progress,
              file_name: fileName || progress.file_name,
            });
          }
        );
      } else {
        const insertSql = `INSERT INTO chat.messages (client_id, conversation_id, sender, role, content, status) VALUES ('${clientId}', ${conversationId}, 'admin', 'user', '${safeContent}', 'sent')`;
        console.log('[useMessages] INSERT SQL:', insertSql);
        await client.query(insertSql);
      }
      console.log('[useMessages] Message inserted successfully');

      // Update conversation timestamp
      console.log('[useMessages] Updating conversation timestamp');
      await client.query(
        `UPDATE chat.conversations SET updated_at = NOW() WHERE id = ${conversationId}`
      ).catch((err) => {
        console.warn('[useMessages] Failed to update conversation timestamp:', err);
      }); // Non-critical

      // The subscription will deliver the real message.
      // Replace optimistic message with server version when it arrives.
      // For now just mark as sent.
      setMessages(prev =>
        prev.map(m => m.client_id === clientId ? { ...m, status: 'sent' as const } : m)
      );

      // Set waiting state to show AI typing indicator
      lastUserMessageTimeRef.current = Date.now();
      setWaitingForAI(true);
      console.log('[useMessages] Waiting for AI response...');

      return optimisticMsg;
    } catch (err) {
      console.error('[useMessages] Failed to send message:', err);
      setMessages(prev =>
        prev.map(m => m.status === 'sending' ? { ...m, status: 'error' as const } : m)
      );
      setError(err instanceof Error ? err.message : 'Failed to send message');
      setWaitingForAI(false); // Clear waiting state on error
      return null;
    } finally {
      setSending(false);
      setUploadProgress(null);
    }
  }, [client, conversationId]);

  const refetch = useCallback(async () => {
    if (!client || !conversationId) return;
    try {
      const msgs = await client.queryAll(
        `SELECT * FROM chat.messages WHERE conversation_id = ${conversationId} ORDER BY created_at ASC`
      );
      setMessages(mergeMessageRows([], msgs.map(rowToMessage) as RawMessageRow[]));
    } catch (err) {
      console.error('[useMessages] Refetch error:', err);
    }
  }, [client, conversationId]);

  const deleteMessage = useCallback(async (messageId: string) => {
    if (!client) return false;
    try {
      await client.query(`DELETE FROM chat.messages WHERE id = '${messageId}'`);
      setMessages(prev => prev.filter(m => m.id !== messageId));
      return true;
    } catch (err) {
      console.error('[useMessages] Failed to delete message:', err);
      return false;
    }
  }, [client]);

  const stopResponse = useCallback(() => {
    if (waitingForAI) {
      setWaitingForAI(false);
      if (waitingTimeoutRef.current) {
        clearTimeout(waitingTimeoutRef.current);
        waitingTimeoutRef.current = null;
      }
    }
  }, [waitingForAI]);

  return { messages, loading, error, sending, uploadProgress, waitingForAI, sendMessage, refetch, deleteMessage, stopResponse };
}

// ============================================================================
// useTypingIndicator - Subscribe to typing events (real-time)
// ============================================================================

export function useTypingIndicator(conversationId: string | null) {
  const { client, isReady } = useKalamDB();
  const [typingUsers, setTypingUsers] = useState<string[]>([]);
  const [aiStatus, setAiStatus] = useState<AiTypingStatus | null>(null);
  const unsubRef = useRef<Unsubscribe | null>(null);

  useEffect(() => {
    console.log('[useTypingIndicator] Effect triggered, conversationId:', conversationId, 'client:', !!client, 'isReady:', isReady);
    if (!client || !isReady || !conversationId) {
      console.log('[useTypingIndicator] Not ready or no conversation');
      setTypingUsers([]);
      setAiStatus(null);
      return;
    }

    let cancelled = false;

    const setupSubscription = async () => {
      if (unsubRef.current) {
        console.log('[useTypingIndicator] Cleaning up previous subscription');
        await unsubRef.current().catch(() => {});
        unsubRef.current = null;
      }

      try {
        console.log('[useTypingIndicator] Setting up subscription for conversation:', conversationId);
        // Stream tables are append-only - query all typing records for this conversation
        // and filter client-side for latest state per user
        const sql = `SELECT * FROM chat.typing_indicators WHERE conversation_id = ${conversationId}`;
        console.log('[useTypingIndicator] SQL:', sql);
        const unsub = await client.subscribeWithSql(
          sql,
          (event: ServerMessage) => {
            if (cancelled) return;
            console.log('[useTypingIndicator] Subscription event:', event.type, event);

            if (event.type === 'initial_data_batch' || event.type === 'change') {
              if ('rows' in event && event.rows) {
                const indicators = event.rows as unknown as TypingIndicator[];
                console.log('[useTypingIndicator] Raw indicators:', indicators.length);
                
                // Stream tables are append-only - find latest state per user
                const latestByUser = new Map<string, TypingIndicator>();
                for (const indicator of indicators) {
                  const existing = latestByUser.get(indicator.user_name);
                  if (!existing || new Date(indicator.updated_at) > new Date(existing.updated_at)) {
                    latestByUser.set(indicator.user_name, indicator);
                  }
                }
                
                // Filter for users currently typing
                const typingUsersList = Array.from(latestByUser.values())
                  .filter(ind => ind.is_typing)
                  .map(ind => ind.user_name);

                const aiIndicator = Array.from(latestByUser.values()).find(ind =>
                  ind.user_name.toLowerCase().includes('ai') || ind.user_name.toLowerCase().includes('assistant')
                );
                setAiStatus(aiIndicator ? parseAiTypingStatus(aiIndicator) : null);
                
                console.log('[useTypingIndicator] Typing users:', typingUsersList);
                setTypingUsers(typingUsersList);
              }
            }
          }
        );

        console.log('[useTypingIndicator] Subscription created successfully');
        unsubRef.current = unsub;
      } catch (err) {
        console.error('[useTypingIndicator] Subscription error:', err);
        console.error('[useTypingIndicator] Error details:', err instanceof Error ? err.stack : err);
        // Typing indicators are non-critical, silently ignore
      }
    };

    setupSubscription();

    return () => {
      cancelled = true;
      if (unsubRef.current) {
        unsubRef.current().catch(() => {});
        unsubRef.current = null;
      }
    };
  }, [client, isReady, conversationId]);

  const setTyping = useCallback(async (isTyping: boolean) => {
    if (!client || !conversationId) return;
    try {
      console.log('[useTypingIndicator] Setting typing:', isTyping);
      // Stream tables don't support DELETE/UPDATE - just insert new state records
      const state = isTyping ? 'typing' : 'finished';
      const insertSql = `INSERT INTO chat.typing_indicators (conversation_id, user_name, is_typing, state) VALUES (${conversationId}, 'admin', ${isTyping}, '${state}')`;
      console.log('[useTypingIndicator] INSERT SQL:', insertSql);
      await client.query(insertSql).catch((err) => {
        console.warn('[useTypingIndicator] Failed to set typing indicator:', err);
      }); // Non-critical
      console.log('[useTypingIndicator] Typing indicator set:', state);
    } catch (err) {
      console.warn('[useTypingIndicator] Failed to set typing indicator:', err);
      // Typing indicators are non-critical
    }
  }, [client, conversationId]);

  return { typingUsers, setTyping, aiStatus };
}

// ============================================================================
// useStreamingText - Simulate token-by-token text streaming animation
// ============================================================================

export function useStreamingText(
  text: string,
  isStreaming: boolean,
  tokenDelayMs: number = 30
) {
  const [displayedText, setDisplayedText] = useState('');
  const [isAnimating, setIsAnimating] = useState(false);
  const indexRef = useRef(0);
  const intervalRef = useRef<NodeJS.Timeout | null>(null);

  useEffect(() => {
    if (!isStreaming || !text) {
      setDisplayedText(text);
      setIsAnimating(false);
      return;
    }

    setIsAnimating(true);
    indexRef.current = 0;
    setDisplayedText('');

    intervalRef.current = setInterval(() => {
      indexRef.current++;
      if (indexRef.current >= text.length) {
        setDisplayedText(text);
        setIsAnimating(false);
        if (intervalRef.current) {
          clearInterval(intervalRef.current);
        }
      } else {
        const nextSpace = text.indexOf(' ', indexRef.current);
        const endIdx = nextSpace !== -1 && nextSpace - indexRef.current < 8
          ? nextSpace + 1
          : indexRef.current + 1;
        indexRef.current = endIdx;
        setDisplayedText(text.substring(0, endIdx));
      }
    }, tokenDelayMs);

    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
      }
    };
  }, [text, isStreaming, tokenDelayMs]);

  return { displayedText, isAnimating };
}

// ============================================================================
// useConnectionStatus - Connection status from KalamDB provider
// ============================================================================

export function useConnectionStatus(): ConnectionStatus {
  const { connectionStatus } = useKalamDB();
  return connectionStatus;
}

// ============================================================================
// Helpers
// ============================================================================

type RawMessageRow = Record<string, unknown> & Message & {
  client_id?: string;
  file_data?: string | FileRef;
};

function mergeMessageRows(existing: Message[], incoming: RawMessageRow[]): Message[] {
  const next = new Map<string, Message>();
  for (const message of existing) {
    const key = message.client_id || message.id;
    next.set(key, message);
  }

  for (const row of incoming) {
    const normalized = normalizeMessageRow(row);
    const key = normalized.client_id || normalized.id;
    const current = next.get(key);
    next.set(key, current ? mergeMessages(current, normalized) : normalized);
  }

  return sortByCreatedAt(Array.from(next.values()));
}

function removeMessageRows(existing: Message[], deleted: RawMessageRow[]): Message[] {
  const deletedIds = new Set(deleted.map(row => row.id));
  const deletedClientIds = new Set(deleted.map(row => row.client_id).filter(Boolean) as string[]);
  return existing.filter(message => !deletedIds.has(message.id) && (!message.client_id || !deletedClientIds.has(message.client_id)));
}

function normalizeMessageRow(row: RawMessageRow): Message {
  const existingFiles = row.files;
  const file = parseFileRef(row.file_data);
  const files = existingFiles || (file ? [fileRefToAttachment(file)] : undefined);
  return {
    id: row.id,
    client_id: row.client_id,
    conversation_id: row.conversation_id,
    sender: row.sender,
    role: row.role,
    content: row.content || '',
    status: row.status as Message['status'],
    created_at: row.created_at,
    files,
  };
}

function mergeMessages(current: Message, incoming: Message): Message {
  const files = mergeFiles(current.files, incoming.files);
  return {
    ...current,
    ...incoming,
    content: incoming.content || current.content,
    files,
  };
}

function mergeFiles(current?: FileAttachment[], incoming?: FileAttachment[]): FileAttachment[] | undefined {
  if (!current && !incoming) return undefined;
  const next = new Map<string, FileAttachment>();
  for (const file of current || []) {
    next.set(file.id, file);
  }
  for (const file of incoming || []) {
    next.set(file.id, file);
  }
  return Array.from(next.values());
}

function parseFileRef(value: string | FileRef | undefined): FileRef | null {
  if (!value) return null;
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value) as FileRef;
      return parsed?.id ? parsed : null;
    } catch {
      return null;
    }
  }
  return value.id ? value : null;
}

function fileRefToAttachment(fileRef: FileRef): FileAttachment {
  const baseUrl = KALAMDB_CONFIG.url.replace(/\/$/, '');
  const url = `${baseUrl}/v1/files/chat/messages/${fileRef.sub}/${fileRef.id}`;
  return {
    id: fileRef.id,
    name: fileRef.name,
    size: fileRef.size,
    mime_type: fileRef.mime,
    url,
  };
}

function createOptimisticFiles(files: File[]): FileAttachment[] {
  return files.map((file, index) => ({
    id: `temp-file-${Date.now()}-${index}`,
    name: file.name,
    size: file.size,
    mime_type: file.type || 'application/octet-stream',
    url: URL.createObjectURL(file),
  }));
}

function createClientId(): string {
  if (typeof crypto !== 'undefined' && 'randomUUID' in crypto) {
    return crypto.randomUUID();
  }
  return `client-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function parseAiTypingStatus(indicator: TypingIndicator): AiTypingStatus {
  const rawState = (indicator.state || '').trim().toLowerCase();
  const typingWithTokens = /^typing:(\d+)$/.exec(rawState);
  if (typingWithTokens) {
    const tokens = Number(typingWithTokens[1]);
    return {
      phase: 'typing',
      isTyping: indicator.is_typing,
      tokens: Number.isFinite(tokens) ? tokens : undefined,
      label: Number.isFinite(tokens) ? `Typing • ${tokens} tok` : 'Typing',
    };
  }

  if (rawState === 'thinking') {
    return {
      phase: 'thinking',
      isTyping: indicator.is_typing,
      label: 'Thinking',
    };
  }

  if (rawState === 'typing') {
    return {
      phase: 'typing',
      isTyping: indicator.is_typing,
      label: 'Typing',
    };
  }

  if (rawState === 'finished') {
    return {
      phase: 'finished',
      isTyping: false,
      label: 'Finished',
    };
  }

  return {
    phase: 'unknown',
    isTyping: indicator.is_typing,
    label: indicator.is_typing ? 'Working' : 'Idle',
  };
}

/** Merge new rows into existing array, avoiding duplicates by id */
function mergeRows<T extends { id: string }>(existing: T[], incoming: T[]): T[] {
  const existingIds = new Set(existing.map(r => r.id));
  const newRows = incoming.filter(r => !existingIds.has(r.id));
  return [...existing, ...newRows];
}

/** Sort rows by created_at ascending */
function sortByCreatedAt<T extends { created_at: string }>(rows: T[]): T[] {
  return [...rows].sort((a, b) => {
    const left = parseTimestamp(a.created_at)?.getTime() ?? 0;
    const right = parseTimestamp(b.created_at)?.getTime() ?? 0;
    return left - right;
  });
}
