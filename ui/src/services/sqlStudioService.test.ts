import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@/lib/kalam-client", () => ({
  executeQuery: vi.fn(),
  executeSql: vi.fn(),
}));

import { executeSql } from "@/lib/kalam-client";
import { fetchSqlStudioSchemaTree } from "@/services/sqlStudioService";

const executeSqlMock = vi.mocked(executeSql);

describe("fetchSqlStudioSchemaTree", () => {
  beforeEach(() => {
    executeSqlMock.mockReset();
  });

  it("includes queryable system tables even when persisted metadata omits the system namespace", async () => {
    executeSqlMock.mockImplementation(async (sql: string) => {
      if (sql.includes("FROM system.namespaces")) {
        return [{ namespace_id: "default" }];
      }

      if (sql.includes("FROM system.tables t")) {
        return [
          {
            namespace_id: "default",
            table_name: "events",
            table_type: "shared",
            column_name: "id",
            data_type: "BigInt",
            nullable: false,
            primary_key: true,
            ordinal: 1,
          },
        ];
      }

      if (sql.includes("FROM information_schema.tables")) {
        return [
          {
            namespace_id: "system",
            table_name: "users",
            table_type: "BASE TABLE",
          },
          {
            namespace_id: "system",
            table_name: "jobs",
            table_type: "BASE TABLE",
          },
        ];
      }

      if (sql.includes("FROM information_schema.columns")) {
        return [
          {
            namespace_id: "system",
            table_name: "users",
            column_name: "user_id",
            data_type: "Utf8",
            is_nullable: "NO",
            ordinal_position: 1,
          },
          {
            namespace_id: "system",
            table_name: "jobs",
            column_name: "job_id",
            data_type: "Utf8",
            is_nullable: "NO",
            ordinal_position: 1,
          },
        ];
      }

      throw new Error(`Unexpected SQL: ${sql}`);
    });

    const schema = await fetchSqlStudioSchemaTree();

    expect(schema.map((namespace) => namespace.name)).toEqual(["default", "system"]);

    expect(schema[0]).toMatchObject({
      name: "default",
      tables: [{
        name: "events",
        tableType: "shared",
      }],
    });

    expect(schema[1]).toMatchObject({
      name: "system",
      tables: [
        {
          name: "jobs",
          tableType: "system",
        },
        {
          name: "users",
          tableType: "system",
        },
      ],
    });

    expect(schema[1].tables[0].columns[0]).toMatchObject({
      name: "job_id",
      isNullable: false,
      ordinal: 1,
    });
    expect(schema[1].tables[1].columns[0]).toMatchObject({
      name: "user_id",
      isNullable: false,
      ordinal: 1,
    });
  });
});
