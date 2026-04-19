import { describe, expect, it } from "vitest";
import { mapUsers } from "@/services/userService";

describe("mapUsers", () => {
  it("uses user_id as username fallback when username column is absent", () => {
    const users = mapUsers([
      {
        user_id: "root",
        role: "system",
        email: "root@localhost",
        auth_type: "password",
        auth_data: null,
        storage_mode: "table",
        storage_id: null,
        failed_login_attempts: 0,
        locked_until: null,
        last_login_at: null,
        last_seen: null,
        created_at: "2026-01-01T00:00:00Z",
        updated_at: "2026-01-01T00:00:00Z",
        deleted_at: null,
      },
    ]);

    expect(users).toHaveLength(1);
    expect(users[0].user_id).toBe("root");
    expect(users[0].username).toBe("root");
  });
});
