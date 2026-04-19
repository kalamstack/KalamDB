import { createApi, fakeBaseQuery } from "@reduxjs/toolkit/query/react";
import { fetchSqlStudioSchemaTree } from "@/services/sqlStudioService";
import { fetchJobs, type Job } from "@/services/jobService";
import {
  fetchAuditLogs,
  type AuditLog,
  type AuditLogFilters,
} from "@/services/auditLogService";
import {
  fetchServerLogs,
  type ServerLog,
  type ServerLogFilters,
} from "@/services/serverLogService";
import {
  fetchLiveQueries,
  killLiveQuery,
  type LiveQuery,
  type LiveQueryFilters,
} from "@/services/liveQueryService";
import {
  fetchNotifications,
  type NotificationsPayload,
} from "@/services/notificationService";
import {
  fetchClusterSnapshot,
  type ClusterSnapshot,
} from "@/services/clusterService";
import {
  checkStorageHealth,
  createStorage,
  fetchStorages,
  type CreateStorageInput,
  type Storage,
  type StorageHealthResult,
  type UpdateStorageInput,
  updateStorage,
} from "@/services/storageService";
import {
  fetchSystemSettings,
  fetchSystemStats,
  fetchDbaStats,
  type DbaStatRow,
  type Setting,
  type SystemStatsMap,
} from "@/services/systemTableService";
import {
  createUser,
  deleteUser,
  fetchUsers,
  type CreateUserInput,
  type UpdateUserInput,
  type User,
  updateUser,
} from "@/services/userService";
import {
  consumeTopicMessages,
  fetchStreamingConsumerGroups,
  fetchStreamingOffsets,
  fetchStreamingTopics,
} from "@/features/streaming/service";
import type { StreamingOffsetsFilter } from "@/features/streaming/sql";
import type {
  ConsumedMessageBatch,
  ConsumeMessagesInput,
  StreamingConsumerGroup,
  StreamingOffset,
  StreamingTopic,
} from "@/features/streaming/types";
import type { JobFilters } from "@/services/sql/queries/jobQueries";

interface CustomQueryError {
  status: "CUSTOM_ERROR";
  error: string;
}

export const apiSlice = createApi({
  reducerPath: "api",
  baseQuery: fakeBaseQuery<CustomQueryError>(),
  tagTypes: [
    "Settings",
    "Stats",
    "Users",
    "Jobs",
    "AuditLogs",
    "ServerLogs",
    "LiveQueries",
    "Notifications",
    "Cluster",
    "Storages",
    "SqlStudioSchema",
    "StreamingTopics",
    "StreamingGroups",
    "StreamingOffsets",
  ],
  endpoints: (builder) => ({
    getSettings: builder.query<Setting[], void>({
      async queryFn() {
        try {
          const data = await fetchSystemSettings();
          return { data };
        } catch (error) {
          const message = error instanceof Error ? error.message : "Failed to fetch settings";
          return { error: { status: "CUSTOM_ERROR", error: message } };
        }
      },
      providesTags: ["Settings"],
    }),
    getStats: builder.query<SystemStatsMap, void>({
      async queryFn() {
        try {
          const data = await fetchSystemStats();
          return { data };
        } catch (error) {
          const message = error instanceof Error ? error.message : "Failed to fetch stats";
          return { error: { status: "CUSTOM_ERROR", error: message } };
        }
      },
      providesTags: ["Stats"],
    }),
    getDbaStats: builder.query<DbaStatRow[], string>({
      async queryFn(timeRange) {
        try {
          const data = await fetchDbaStats(timeRange || "24 HOURS");
          return { data };
        } catch (error) {
          const message = error instanceof Error ? error.message : "Failed to fetch dba stats";
          return { error: { status: "CUSTOM_ERROR", error: message } };
        }
      },
      providesTags: ["Stats"],
    }),
    getUsersList: builder.query<User[], void>({
      async queryFn() {
        try {
          const data = await fetchUsers();
          return { data };
        } catch (error) {
          const message = error instanceof Error ? error.message : "Failed to fetch users";
          return { error: { status: "CUSTOM_ERROR", error: message } };
        }
      },
      providesTags: ["Users"],
    }),
    createUser: builder.mutation<void, CreateUserInput>({
      async queryFn(input) {
        try {
          await createUser(input);
          return { data: undefined };
        } catch (error) {
          const message = error instanceof Error ? error.message : "Failed to create user";
          return { error: { status: "CUSTOM_ERROR", error: message } };
        }
      },
      invalidatesTags: ["Users"],
    }),
    updateUser: builder.mutation<void, { username: string; input: UpdateUserInput }>({
      async queryFn({ username, input }) {
        try {
          await updateUser(username, input);
          return { data: undefined };
        } catch (error) {
          const message = error instanceof Error ? error.message : "Failed to update user";
          return { error: { status: "CUSTOM_ERROR", error: message } };
        }
      },
      invalidatesTags: ["Users"],
    }),
    deleteUser: builder.mutation<void, { username: string }>({
      async queryFn({ username }) {
        try {
          await deleteUser(username);
          return { data: undefined };
        } catch (error) {
          const message = error instanceof Error ? error.message : "Failed to delete user";
          return { error: { status: "CUSTOM_ERROR", error: message } };
        }
      },
      invalidatesTags: ["Users"],
    }),
    getJobsFiltered: builder.query<Job[], JobFilters | void>({
      async queryFn(filters) {
        try {
          const normalizedFilters = filters && typeof filters === "object" ? filters : undefined;
          const data = await fetchJobs(normalizedFilters);
          return { data };
        } catch (error) {
          const message = error instanceof Error ? error.message : "Failed to fetch jobs";
          return { error: { status: "CUSTOM_ERROR", error: message } };
        }
      },
      providesTags: ["Jobs"],
    }),
    getAuditLogs: builder.query<AuditLog[], AuditLogFilters | void>({
      async queryFn(filters) {
        try {
          const normalizedFilters = filters && typeof filters === "object" ? filters : undefined;
          const data = await fetchAuditLogs(normalizedFilters);
          return { data };
        } catch (error) {
          const message = error instanceof Error ? error.message : "Failed to fetch audit logs";
          return { error: { status: "CUSTOM_ERROR", error: message } };
        }
      },
      providesTags: ["AuditLogs"],
    }),
    getServerLogs: builder.query<ServerLog[], ServerLogFilters | void>({
      async queryFn(filters) {
        try {
          const normalizedFilters = filters && typeof filters === "object" ? filters : undefined;
          const data = await fetchServerLogs(normalizedFilters);
          return { data };
        } catch (error) {
          const message = error instanceof Error ? error.message : "Failed to fetch server logs";
          return { error: { status: "CUSTOM_ERROR", error: message } };
        }
      },
      providesTags: ["ServerLogs"],
    }),
    getLiveQueries: builder.query<LiveQuery[], LiveQueryFilters | void>({
      async queryFn(filters) {
        try {
          const normalizedFilters = filters && typeof filters === "object" ? filters : undefined;
          const data = await fetchLiveQueries(normalizedFilters);
          return { data };
        } catch (error) {
          const message = error instanceof Error ? error.message : "Failed to fetch live queries";
          return { error: { status: "CUSTOM_ERROR", error: message } };
        }
      },
      providesTags: ["LiveQueries"],
    }),
    killLiveQuery: builder.mutation<void, string>({
      async queryFn(liveId) {
        try {
          await killLiveQuery(liveId);
          return { data: undefined };
        } catch (error) {
          const message = error instanceof Error ? error.message : "Failed to kill live query";
          return { error: { status: "CUSTOM_ERROR", error: message } };
        }
      },
      invalidatesTags: ["LiveQueries"],
    }),
    getNotifications: builder.query<NotificationsPayload, void>({
      async queryFn() {
        try {
          const data = await fetchNotifications();
          return { data };
        } catch (error) {
          const message = error instanceof Error ? error.message : "Failed to fetch notifications";
          return { error: { status: "CUSTOM_ERROR", error: message } };
        }
      },
      providesTags: ["Notifications"],
    }),
    getClusterSnapshot: builder.query<ClusterSnapshot, void>({
      async queryFn() {
        try {
          const data = await fetchClusterSnapshot();
          return { data };
        } catch (error) {
          const message = error instanceof Error ? error.message : "Failed to fetch cluster information";
          return { error: { status: "CUSTOM_ERROR", error: message } };
        }
      },
      providesTags: ["Cluster"],
    }),
    getStorages: builder.query<Storage[], void>({
      async queryFn() {
        try {
          const data = await fetchStorages();
          return { data };
        } catch (error) {
          const message = error instanceof Error ? error.message : "Failed to fetch storages";
          return { error: { status: "CUSTOM_ERROR", error: message } };
        }
      },
      providesTags: ["Storages"],
    }),
    createStorage: builder.mutation<void, CreateStorageInput>({
      async queryFn(input) {
        try {
          await createStorage(input);
          return { data: undefined };
        } catch (error) {
          const message = error instanceof Error ? error.message : "Failed to create storage";
          return { error: { status: "CUSTOM_ERROR", error: message } };
        }
      },
      invalidatesTags: ["Storages"],
    }),
    updateStorage: builder.mutation<void, { storageId: string; input: UpdateStorageInput }>({
      async queryFn({ storageId, input }) {
        try {
          await updateStorage(storageId, input);
          return { data: undefined };
        } catch (error) {
          const message = error instanceof Error ? error.message : "Failed to update storage";
          return { error: { status: "CUSTOM_ERROR", error: message } };
        }
      },
      invalidatesTags: ["Storages"],
    }),
    checkStorageHealth: builder.mutation<StorageHealthResult, { storageId: string; extended?: boolean }>({
      async queryFn({ storageId, extended }) {
        try {
          const data = await checkStorageHealth(storageId, extended ?? true);
          return { data };
        } catch (error) {
          const message = error instanceof Error ? error.message : "Failed to check storage health";
          return { error: { status: "CUSTOM_ERROR", error: message } };
        }
      },
    }),
    getSqlStudioSchemaTree: builder.query<Awaited<ReturnType<typeof fetchSqlStudioSchemaTree>>, void>({
      async queryFn() {
        try {
          const data = await fetchSqlStudioSchemaTree();
          return { data };
        } catch (error) {
          const message = error instanceof Error ? error.message : "Failed to fetch SQL Studio schema";
          return { error: { status: "CUSTOM_ERROR", error: message } };
        }
      },
      providesTags: ["SqlStudioSchema"],
    }),
    getStreamingTopics: builder.query<StreamingTopic[], void>({
      async queryFn() {
        try {
          const data = await fetchStreamingTopics();
          return { data };
        } catch (error) {
          const message = error instanceof Error ? error.message : "Failed to fetch streaming topics";
          return { error: { status: "CUSTOM_ERROR", error: message } };
        }
      },
      providesTags: ["StreamingTopics"],
    }),
    getStreamingConsumerGroups: builder.query<StreamingConsumerGroup[], void>({
      async queryFn() {
        try {
          const data = await fetchStreamingConsumerGroups();
          return { data };
        } catch (error) {
          const message = error instanceof Error ? error.message : "Failed to fetch streaming consumer groups";
          return { error: { status: "CUSTOM_ERROR", error: message } };
        }
      },
      providesTags: ["StreamingGroups"],
    }),
    getStreamingOffsets: builder.query<StreamingOffset[], StreamingOffsetsFilter | void>({
      async queryFn(filters) {
        try {
          const normalizedFilters = filters && typeof filters === "object" ? filters : undefined;
          const data = await fetchStreamingOffsets(normalizedFilters);
          return { data };
        } catch (error) {
          const message = error instanceof Error ? error.message : "Failed to fetch streaming offsets";
          return { error: { status: "CUSTOM_ERROR", error: message } };
        }
      },
      providesTags: ["StreamingOffsets"],
    }),
    consumeStreamingMessages: builder.mutation<ConsumedMessageBatch, ConsumeMessagesInput>({
      async queryFn(input) {
        try {
          const data = await consumeTopicMessages(input);
          return { data };
        } catch (error) {
          const message = error instanceof Error ? error.message : "Failed to consume topic messages";
          return { error: { status: "CUSTOM_ERROR", error: message } };
        }
      },
    }),
  }),
});

export const {
  useGetSettingsQuery,
  useGetStatsQuery,
  useGetDbaStatsQuery,
  useGetUsersListQuery,
  useCreateUserMutation,
  useUpdateUserMutation,
  useDeleteUserMutation,
  useGetJobsFilteredQuery,
  useGetAuditLogsQuery,
  useGetServerLogsQuery,
  useGetLiveQueriesQuery,
  useKillLiveQueryMutation,
  useGetNotificationsQuery,
  useGetClusterSnapshotQuery,
  useGetStoragesQuery,
  useCreateStorageMutation,
  useUpdateStorageMutation,
  useCheckStorageHealthMutation,
  useGetSqlStudioSchemaTreeQuery,
  useGetStreamingTopicsQuery,
  useGetStreamingConsumerGroupsQuery,
  useGetStreamingOffsetsQuery,
  useConsumeStreamingMessagesMutation,
} = apiSlice;
