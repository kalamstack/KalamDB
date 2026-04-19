import { customType } from 'drizzle-orm/pg-core';
import { FileRef } from '@kalamdb/client';

export const file = customType<{ data: FileRef | null; driverData: string | null }>({
  dataType() {
    return 'text';
  },
  fromDriver(value: string | null): FileRef | null {
    return FileRef.from(value);
  },
  toDriver(value: FileRef | null): string | null {
    if (!value) return null;
    return JSON.stringify({
      id: value.id,
      sub: value.sub,
      name: value.name,
      size: value.size,
      mime: value.mime,
      sha256: value.sha256,
      shard: value.shard,
    });
  },
});
