import { useEffect, useMemo, useState } from "react";
import { AlertCircle, Loader2 } from "lucide-react";
import { useCreateUserMutation, useGetStoragesQuery, useUpdateUserMutation } from "@/store/apiSlice";
import type { User } from "@/services/userService";
import { formatTimestamp } from "@/lib/formatters";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

interface UserFormProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  user?: User;
  onSuccess: () => void;
}

const ROLES = ["user", "service", "dba", "system"] as const;
const AUTH_TYPES = ["password", "oauth", "internal"] as const;
const STORAGE_MODES = ["table", "region"] as const;
const NONE_STORAGE_ID = "__none__";

interface FormData {
  username: string;
  password: string;
  role: string;
  email: string;
  authType: string;
  authData: string;
  storageMode: "table" | "region";
  storageId: string;
}

function formatSystemValue(value: string | number | null | undefined): string {
  if (value === null || value === undefined) {
    return "N/A";
  }
  return String(value);
}

function formatTimestampValue(value: string | number | null | undefined): string {
  if (!value) {
    return "N/A";
  }

  return formatTimestamp(value, undefined, "iso8601-datetime", "utc");
}

function FieldHelp({ text }: { text: string }) {
  return <p className="text-xs text-muted-foreground">{text}</p>;
}

interface SystemFieldProps {
  label: string;
  value: string | number | null | undefined;
  description: string;
  monospace?: boolean;
  breakAll?: boolean;
}

function SystemField({ label, value, description, monospace, breakAll }: SystemFieldProps) {
  return (
    <div className="space-y-1">
      <p className="text-xs text-muted-foreground">{label}</p>
      <p className={["text-sm", monospace ? "font-mono" : "", breakAll ? "break-all" : ""].join(" ").trim()}>
        {formatSystemValue(value)}
      </p>
      <p className="text-xs text-muted-foreground">{description}</p>
    </div>
  );
}

export function UserForm({ open, onOpenChange, user, onSuccess }: UserFormProps) {
  const [createUserMutation] = useCreateUserMutation();
  const [updateUserMutation] = useUpdateUserMutation();
  const { data: storages = [] } = useGetStoragesQuery();
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [formData, setFormData] = useState<FormData>({
    username: "",
    password: "",
    role: "user",
    email: "",
    authType: "password",
    authData: "",
    storageMode: "table",
    storageId: "",
  });

  const isEditing = Boolean(user);

  useEffect(() => {
    if (!open) {
      return;
    }

    setFormData({
      username: user?.username ?? "",
      password: "",
      role: user?.role ?? "user",
      email: user?.email ?? "",
      authType: user?.auth_type ?? "password",
      authData: user?.auth_data ?? "",
      storageMode: user?.storage_mode === "region" ? "region" : "table",
      storageId: user?.storage_id ?? "",
    });
    setError(null);
  }, [open, user]);

  const showPasswordField = (!isEditing && formData.authType === "password") || isEditing;
  const showAuthDataField = !isEditing && formData.authType === "oauth";
  const canSubmit = useMemo(() => {
    if (isEditing) {
      return true;
    }

    if (!formData.username.trim()) {
      return false;
    }
    if (formData.authType === "password" && !formData.password.trim()) {
      return false;
    }
    return true;
  }, [isEditing, formData.username, formData.authType, formData.password]);

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    setIsSubmitting(true);
    setError(null);

    try {
      if (isEditing && user) {
        const updateInput: {
          role?: string;
          password?: string;
          email?: string;
          storage_mode?: "table" | "region" | null;
          storage_id?: string | null;
        } = {};
        if (formData.role !== user.role) {
          updateInput.role = formData.role;
        }
        if (formData.password.trim()) {
          updateInput.password = formData.password.trim();
        }
        if ((formData.email || "") !== (user.email || "")) {
          updateInput.email = formData.email.trim();
        }
        if (formData.storageMode !== (user.storage_mode ?? "table")) {
          updateInput.storage_mode = formData.storageMode;
        }
        const normalizedStorageId = formData.storageId.trim() || null;
        if (normalizedStorageId !== (user.storage_id ?? null)) {
          updateInput.storage_id = normalizedStorageId;
        }

        await updateUserMutation({ username: user.username, input: updateInput }).unwrap();
      } else {
        await createUserMutation({
          username: formData.username.trim(),
          password: formData.authType === "password" ? formData.password.trim() : undefined,
          auth_type: formData.authType as "password" | "oauth" | "internal",
          auth_data: formData.authType === "oauth" ? formData.authData.trim() || undefined : undefined,
          role: formData.role,
          email: formData.email.trim() || undefined,
          storage_mode: formData.storageMode,
          storage_id: formData.storageId.trim() || null,
        }).unwrap();
      }

      onSuccess();
      onOpenChange(false);
    } catch (submitError) {
      setError(submitError instanceof Error ? submitError.message : "Failed to save user");
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent side="right" className="flex flex-col sm:max-w-[620px]">
        <SheetHeader>
          <SheetTitle>{isEditing ? "Edit User" : "Create User"}</SheetTitle>
          <SheetDescription>
            {isEditing
              ? "Update account details, role, and storage preferences. Some authentication metadata remains system-managed."
              : "Create a new database user account with auth type and role."}
          </SheetDescription>
        </SheetHeader>

        <form id="user-form" onSubmit={handleSubmit} className="flex min-h-0 flex-1 flex-col">
          <div className="min-h-0 flex-1 space-y-5 overflow-y-auto px-6 py-5">
            {isEditing && user && (
              <div className="space-y-2">
                <label className="text-sm font-medium">User ID</label>
                <Input value={user.user_id} disabled className="font-mono" />
                <FieldHelp text="Stable system identifier for this account." />
              </div>
            )}

            <div className="space-y-2">
              <label className="text-sm font-medium">Username</label>
              <Input
                value={formData.username}
                onChange={(event) => setFormData((prev) => ({ ...prev, username: event.target.value }))}
                disabled={isEditing}
                placeholder="e.g. analyst_01"
                autoFocus={!isEditing}
              />
              <FieldHelp text={isEditing ? "Username cannot be changed after creation." : "Unique user login name."} />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">Role</label>
              <Select
                value={formData.role}
                onValueChange={(value) => setFormData((prev) => ({ ...prev, role: value }))}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {ROLES.map((role) => (
                    <SelectItem key={role} value={role}>
                      {role}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <FieldHelp text="Access level for this account: user, service, dba, or system." />
            </div>

            {!isEditing && (
              <div className="space-y-2">
                <label className="text-sm font-medium">Auth Type</label>
                <Select
                  value={formData.authType}
                  onValueChange={(value) => setFormData((prev) => ({ ...prev, authType: value }))}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {AUTH_TYPES.map((authType) => (
                      <SelectItem key={authType} value={authType}>
                        {authType}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <FieldHelp text="Authentication mode used by this user (password, oauth, or internal)." />
              </div>
            )}

            {showPasswordField && (
              <div className="space-y-2">
                <label className="text-sm font-medium">
                  Password {isEditing ? "(leave empty to keep current password)" : ""}
                </label>
                <Input
                  type="password"
                  value={formData.password}
                  onChange={(event) => setFormData((prev) => ({ ...prev, password: event.target.value }))}
                  placeholder={isEditing ? "••••••••" : "Enter password"}
                />
                <FieldHelp text="Minimum 8 characters recommended." />
              </div>
            )}

            {showAuthDataField && (
              <div className="space-y-2">
                <label className="text-sm font-medium">Auth Data (OAuth payload)</label>
                <Textarea
                  value={formData.authData}
                  onChange={(event) => setFormData((prev) => ({ ...prev, authData: event.target.value }))}
                  placeholder='e.g. {"provider":"google","subject":"user-123"}'
                  rows={4}
                  className="font-mono text-xs"
                />
                <FieldHelp text="Optional auth payload stored with OAuth users." />
              </div>
            )}

            <div className="space-y-2">
              <label className="text-sm font-medium">Email</label>
              <Input
                type="email"
                value={formData.email}
                onChange={(event) => setFormData((prev) => ({ ...prev, email: event.target.value }))}
                placeholder="user@example.com"
              />
              <FieldHelp text="Optional contact email for this user." />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">Storage Mode</label>
              <Select
                value={formData.storageMode}
                onValueChange={(value) =>
                  setFormData((prev) => ({ ...prev, storageMode: value as "table" | "region" }))
                }
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {STORAGE_MODES.map((mode) => (
                    <SelectItem key={mode} value={mode}>
                      {mode}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <FieldHelp text="Controls how this user resolves storage placement (table or region)." />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">Storage ID</label>
              <Select
                value={formData.storageId || NONE_STORAGE_ID}
                onValueChange={(value) =>
                  setFormData((prev) => ({ ...prev, storageId: value === NONE_STORAGE_ID ? "" : value }))
                }
              >
                <SelectTrigger>
                  <SelectValue placeholder="Select storage target" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value={NONE_STORAGE_ID}>No preferred storage</SelectItem>
                  {storages.map((storage) => (
                    <SelectItem key={storage.storage_id} value={storage.storage_id}>
                      {storage.storage_name} ({storage.storage_id})
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <FieldHelp text="Optional preferred storage configuration for this user." />
            </div>

            {isEditing && user && (
              <div className="space-y-3 rounded-md border bg-muted/40 p-4">
                <h3 className="text-sm font-semibold">System Fields (Read-only)</h3>
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                  <SystemField
                    label="Auth Type"
                    value={user.auth_type}
                    description="Current authentication strategy used by this user."
                  />
                  <SystemField
                    label="Auth Data"
                    value={user.auth_data}
                    description="Auth provider payload (OAuth/internal metadata)."
                    monospace
                    breakAll
                  />
                  <SystemField
                    label="Storage Mode"
                    value={user.storage_mode}
                    description="How user data is partitioned for storage selection."
                  />
                  <SystemField
                    label="Storage ID"
                    value={user.storage_id}
                    description="Preferred storage target when configured."
                  />
                  <SystemField
                    label="Failed Login Attempts"
                    value={user.failed_login_attempts}
                    description="Consecutive authentication failures before lockout."
                  />
                  <SystemField
                    label="Locked Until"
                    value={formatTimestampValue(user.locked_until)}
                    description="Lockout expiration timestamp if account is temporarily blocked."
                  />
                  <SystemField
                    label="Last Login"
                    value={formatTimestampValue(user.last_login_at)}
                    description="Most recent successful login timestamp."
                  />
                  <SystemField
                    label="Last Seen"
                    value={formatTimestampValue(user.last_seen)}
                    description="Latest authenticated activity timestamp."
                  />
                  <SystemField
                    label="Created At"
                    value={formatTimestampValue(user.created_at)}
                    description="Account creation timestamp."
                  />
                  <SystemField
                    label="Updated At"
                    value={formatTimestampValue(user.updated_at)}
                    description="Last profile update timestamp."
                  />
                </div>
                <FieldHelp text="These fields are currently system-managed and displayed for visibility." />
              </div>
            )}

            {error && (
              <Alert variant="destructive">
                <AlertCircle className="h-4 w-4" />
                <AlertTitle>Save failed</AlertTitle>
                <AlertDescription>{error}</AlertDescription>
              </Alert>
            )}
          </div>
        </form>

        <SheetFooter>
          <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button form="user-form" type="submit" disabled={isSubmitting || !canSubmit}>
            {isSubmitting && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
            {isEditing ? "Save Changes" : "Create User"}
          </Button>
        </SheetFooter>
      </SheetContent>
    </Sheet>
  );
}
