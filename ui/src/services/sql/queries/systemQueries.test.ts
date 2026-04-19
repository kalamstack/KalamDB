import { describe, expect, it } from "vitest";
import { SYSTEM_SETTINGS_QUERY, SYSTEM_USERS_QUERY } from "@/services/sql/queries/systemQueries";

describe("system queries", () => {
  it("loads settings using SELECT * from system.settings", () => {
    expect(SYSTEM_SETTINGS_QUERY).toMatch(/SELECT\s+\*/i);
    expect(SYSTEM_SETTINGS_QUERY).toMatch(/FROM\s+system\.settings/i);
  });

  it("queries system.users by canonical user_id column", () => {
    expect(SYSTEM_USERS_QUERY).toMatch(/SELECT[\s\S]*\buser_id\b/i);
    expect(SYSTEM_USERS_QUERY).toMatch(/ORDER BY\s+user_id/i);
    expect(SYSTEM_USERS_QUERY).not.toMatch(/\busername\b/i);
  });
});
