export interface CreateUserInput {
  username: string;
  password?: string;
  auth_type?: "password" | "oauth" | "internal";
  auth_data?: string;
  role?: string;
  email?: string;
  storage_mode?: "table" | "region" | null;
  storage_id?: string | null;
}

export interface UpdateUserInput {
  role?: string;
  password?: string;
  email?: string;
  storage_mode?: "table" | "region" | null;
  storage_id?: string | null;
}

function escapeSqlLiteral(value: string): string {
  return value.replace(/'/g, "''");
}

export function buildCreateUserSql(input: CreateUserInput): string {
  const authType = (input.auth_type ?? "password").toLowerCase();
  let sql = `CREATE USER '${escapeSqlLiteral(input.username)}'`;

  if (authType === "oauth") {
    sql += ` WITH OAUTH`;
    if (input.auth_data?.trim()) {
      sql += ` '${escapeSqlLiteral(input.auth_data.trim())}'`;
    }
  } else if (authType === "internal") {
    sql += ` WITH INTERNAL`;
  } else {
    const password = input.password?.trim();
    if (!password) {
      throw new Error("Password is required for password auth type");
    }
    sql += ` WITH PASSWORD '${escapeSqlLiteral(password)}'`;
  }

  if (input.role) {
    sql += ` ROLE '${escapeSqlLiteral(input.role)}'`;
  }
  if (input.email?.trim()) {
    sql += ` EMAIL '${escapeSqlLiteral(input.email.trim())}'`;
  }
  return sql;
}

export function buildUpdateUserRoleSql(username: string, role: string): string {
  return `ALTER USER '${escapeSqlLiteral(username)}' SET ROLE '${escapeSqlLiteral(role)}'`;
}

export function buildUpdateUserPasswordSql(username: string, password: string): string {
  return `ALTER USER '${escapeSqlLiteral(username)}' SET PASSWORD '${escapeSqlLiteral(password)}'`;
}

export function buildUpdateUserEmailSql(username: string, email: string): string {
  return `ALTER USER '${escapeSqlLiteral(username)}' SET EMAIL '${escapeSqlLiteral(email)}'`;
}

export function buildUpdateUserStorageSql(
  username: string,
  storageMode: "table" | "region" | null | undefined,
  storageId: string | null | undefined,
): string | null {
  const setClauses: string[] = [];

  if (storageMode !== undefined) {
    setClauses.push(
      storageMode === null
        ? "storage_mode = NULL"
        : `storage_mode = '${escapeSqlLiteral(storageMode)}'`,
    );
  }

  if (storageId !== undefined) {
    const normalizedStorageId = storageId?.trim() ?? "";
    setClauses.push(
      normalizedStorageId.length === 0
        ? "storage_id = NULL"
        : `storage_id = '${escapeSqlLiteral(normalizedStorageId)}'`,
    );
  }

  if (setClauses.length === 0) {
    return null;
  }

  return `UPDATE system.users SET ${setClauses.join(", ")} WHERE user_id = '${escapeSqlLiteral(username)}'`;
}

export function buildDeleteUserSql(username: string): string {
  return `DROP USER '${escapeSqlLiteral(username)}'`;
}
