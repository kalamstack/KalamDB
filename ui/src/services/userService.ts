import { executeSql } from "@/lib/kalam-client";
import { getDb } from "@/lib/db";
import { system_users } from "@/lib/schema";
import { isNull, asc, type InferSelectModel } from "drizzle-orm";
import {
  buildCreateUserSql,
  buildDeleteUserSql,
  buildUpdateUserEmailSql,
  buildUpdateUserPasswordSql,
  buildUpdateUserRoleSql,
  buildUpdateUserStorageSql,
  type CreateUserInput,
  type UpdateUserInput,
} from "@/services/sql/queries/userQueries";

type UserModel = InferSelectModel<typeof system_users>;
export type User = Omit<UserModel, "password_hash"> & { username: string };

type UserWithoutPasswordHash = User;

type UserWithOptionalUsername = Omit<UserWithoutPasswordHash, "username"> & {
  username?: string | null;
};

export type { CreateUserInput, UpdateUserInput };

export function mapUsers<T extends UserWithOptionalUsername>(rows: T[]): Array<T & { username: string }> {
  return rows.map((row) => ({
    ...row,
    username: row.username ?? row.user_id,
  }));
}

export async function fetchUsers() {
  const db = getDb();
  const rows = await db
    .select({
      user_id: system_users.user_id,
      role: system_users.role,
      email: system_users.email,
      auth_type: system_users.auth_type,
      auth_data: system_users.auth_data,
      storage_mode: system_users.storage_mode,
      storage_id: system_users.storage_id,
      created_at: system_users.created_at,
      updated_at: system_users.updated_at,
      last_seen: system_users.last_seen,
      deleted_at: system_users.deleted_at,
      failed_login_attempts: system_users.failed_login_attempts,
      locked_until: system_users.locked_until,
      last_login_at: system_users.last_login_at,
    })
    .from(system_users)
    .where(isNull(system_users.deleted_at))
    .orderBy(asc(system_users.user_id));

  return mapUsers(rows);
}

export async function createUser(input: CreateUserInput): Promise<void> {
  await executeSql(buildCreateUserSql(input));
  const storageSql = buildUpdateUserStorageSql(input.username, input.storage_mode, input.storage_id);
  if (storageSql) {
    await executeSql(storageSql);
  }
}

export async function updateUser(username: string, input: UpdateUserInput): Promise<void> {
  if (input.role) {
    await executeSql(buildUpdateUserRoleSql(username, input.role));
  }
  if (input.password) {
    await executeSql(buildUpdateUserPasswordSql(username, input.password));
  }
  if (input.email !== undefined) {
    await executeSql(buildUpdateUserEmailSql(username, input.email));
  }
  const storageSql = buildUpdateUserStorageSql(username, input.storage_mode, input.storage_id);
  if (storageSql) {
    await executeSql(storageSql);
  }
}

export async function deleteUser(username: string): Promise<void> {
  await executeSql(buildDeleteUserSql(username));
}
