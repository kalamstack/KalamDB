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

export type User = InferSelectModel<typeof system_users>;

export type { CreateUserInput, UpdateUserInput };

export async function fetchUsers() {
  const db = getDb();
  return db
    .select()
    .from(system_users)
    .where(isNull(system_users.deleted_at))
    .orderBy(asc(system_users.username));
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
