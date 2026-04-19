import { useEffect, useMemo, useState } from "react";
import { AlertCircle, Cloud, Database, HardDrive, Loader2 } from "lucide-react";
import { useCreateStorageMutation, useUpdateStorageMutation } from "@/store/apiSlice";
import type { Storage } from "@/services/storageService";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Textarea } from "@/components/ui/textarea";

interface StorageFormProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  storage?: Storage;
  onSuccess: () => void;
}

type StorageType = "filesystem" | "s3" | "gcs" | "azure";

interface StorageTypeOption {
  value: StorageType;
  label: string;
  description: string;
  icon: React.ComponentType<{ className?: string }>;
  pathLabel: string;
  pathPlaceholder: string;
  pathHelp: string;
}

const STORAGE_TYPE_OPTIONS: StorageTypeOption[] = [
  {
    value: "filesystem",
    label: "Local Filesystem",
    description: "Store data on local or network-attached file systems.",
    icon: HardDrive,
    pathLabel: "Base Directory",
    pathPlaceholder: "/var/data/kalamdb/",
    pathHelp: "Absolute path to the storage directory on the server.",
  },
  {
    value: "s3",
    label: "Amazon S3",
    description: "Store data in AWS S3 buckets.",
    icon: Cloud,
    pathLabel: "Bucket / Prefix",
    pathPlaceholder: "s3://my-bucket/kalamdb/",
    pathHelp: "S3 bucket URI. Prefix with s3:// when possible.",
  },
  {
    value: "gcs",
    label: "Google Cloud Storage",
    description: "Store data in Google Cloud Storage buckets.",
    icon: Cloud,
    pathLabel: "Bucket / Prefix",
    pathPlaceholder: "gs://my-bucket/kalamdb/",
    pathHelp: "GCS bucket URI (gs://bucket/prefix).",
  },
  {
    value: "azure",
    label: "Azure Blob Storage",
    description: "Store data in Azure Blob Storage containers.",
    icon: Database,
    pathLabel: "Container URI",
    pathPlaceholder: "https://account.blob.core.windows.net/container/",
    pathHelp: "Azure container URL used as base location.",
  },
];

interface FormData {
  storage_id: string;
  storage_type: StorageType;
  storage_name: string;
  description: string;
  base_directory: string;
  credentials: string;
  config_json: string;
  shared_tables_template: string;
  user_tables_template: string;
}

function jsonValueToText(value: unknown): string {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "string") {
    return value;
  }
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function FieldHelp({ text }: { text: string }) {
  return <p className="text-xs text-muted-foreground">{text}</p>;
}

export function StorageForm({ open, onOpenChange, storage, onSuccess }: StorageFormProps) {
  const [createStorageMutation] = useCreateStorageMutation();
  const [updateStorageMutation] = useUpdateStorageMutation();
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [formData, setFormData] = useState<FormData>({
    storage_id: "",
    storage_type: "filesystem",
    storage_name: "",
    description: "",
    base_directory: "",
    credentials: "",
    config_json: "",
    shared_tables_template: "{namespace}/{tableName}/",
    user_tables_template: "{namespace}/{tableName}/{userId}/",
  });

  const isEditing = Boolean(storage);
  const selectedType = useMemo(
    () => STORAGE_TYPE_OPTIONS.find((item) => item.value === formData.storage_type) ?? STORAGE_TYPE_OPTIONS[0],
    [formData.storage_type],
  );

  useEffect(() => {
    if (!open) {
      return;
    }

    setFormData({
      storage_id: storage?.storage_id ?? "",
      storage_type: (storage?.storage_type as StorageType) ?? "filesystem",
      storage_name: storage?.storage_name ?? "",
      description: storage?.description ?? "",
      base_directory: storage?.base_directory ?? "",
      credentials: jsonValueToText(storage?.credentials),
      config_json: jsonValueToText(storage?.config_json),
      shared_tables_template: storage?.shared_tables_template ?? "{namespace}/{tableName}/",
      user_tables_template: storage?.user_tables_template ?? "{namespace}/{tableName}/{userId}/",
    });
    setError(null);
  }, [open, storage]);

  const canSubmit = useMemo(() => {
    if (!formData.storage_name.trim()) {
      return false;
    }
    if (!isEditing && (!formData.storage_id.trim() || !formData.base_directory.trim())) {
      return false;
    }
    return true;
  }, [formData.storage_name, formData.storage_id, formData.base_directory, isEditing]);

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    setIsSubmitting(true);
    setError(null);

    try {
      if (isEditing && storage) {
        await updateStorageMutation({
          storageId: storage.storage_id,
          input: {
            storage_name: formData.storage_name !== storage.storage_name ? formData.storage_name : undefined,
            description: formData.description !== (storage.description ?? "") ? formData.description : undefined,
            config_json: formData.config_json !== jsonValueToText(storage.config_json) ? formData.config_json : undefined,
            shared_tables_template:
              formData.shared_tables_template !== (storage.shared_tables_template ?? "")
                ? formData.shared_tables_template
                : undefined,
            user_tables_template:
              formData.user_tables_template !== (storage.user_tables_template ?? "")
                ? formData.user_tables_template
                : undefined,
          },
        }).unwrap();
      } else {
        await createStorageMutation({
          storage_id: formData.storage_id.trim(),
          storage_type: formData.storage_type,
          storage_name: formData.storage_name.trim(),
          description: formData.description.trim() || undefined,
          base_directory: formData.base_directory.trim(),
          credentials: formData.credentials.trim() || undefined,
          config_json: formData.config_json.trim() || undefined,
          shared_tables_template: formData.shared_tables_template.trim() || undefined,
          user_tables_template: formData.user_tables_template.trim() || undefined,
        }).unwrap();
      }

      onSuccess();
      onOpenChange(false);
    } catch (submitError) {
      setError(submitError instanceof Error ? submitError.message : "Failed to save storage");
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent side="right" className="flex flex-col sm:max-w-[620px]">
        <SheetHeader>
          <SheetTitle>{isEditing ? "Edit Storage" : "Create Storage"}</SheetTitle>
          <SheetDescription>
            {isEditing
              ? "Update storage metadata and templates. Immutable fields are shown as read-only."
              : "Create a new storage backend for table files and metadata."}
          </SheetDescription>
        </SheetHeader>

        <form id="storage-form" onSubmit={handleSubmit} className="flex min-h-0 flex-1 flex-col">
          <div className="min-h-0 flex-1 space-y-5 overflow-y-auto px-6 py-5">
            <div className="space-y-2">
              <label className="text-sm font-medium">Storage ID</label>
              <Input
                value={formData.storage_id}
                onChange={(event) => setFormData((prev) => ({ ...prev, storage_id: event.target.value }))}
                disabled={isEditing}
                placeholder="e.g. local_storage, s3_prod"
              />
              <FieldHelp text="Unique identifier. Immutable after creation." />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">Storage Type</label>
              <Select
                value={formData.storage_type}
                onValueChange={(value) => setFormData((prev) => ({ ...prev, storage_type: value as StorageType }))}
                disabled={isEditing}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {STORAGE_TYPE_OPTIONS.map((option) => {
                    const Icon = option.icon;
                    return (
                      <SelectItem key={option.value} value={option.value}>
                        <div className="flex items-start gap-2">
                          <Icon className="mt-0.5 h-4 w-4 text-muted-foreground" />
                          <div>
                            <p>{option.label}</p>
                            <p className="text-xs text-muted-foreground">{option.description}</p>
                          </div>
                        </div>
                      </SelectItem>
                    );
                  })}
                </SelectContent>
              </Select>
              <FieldHelp text="Selected backend provider. Immutable after creation." />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">Storage Name</label>
              <Input
                value={formData.storage_name}
                onChange={(event) => setFormData((prev) => ({ ...prev, storage_name: event.target.value }))}
                placeholder="e.g. Production S3 Storage"
              />
              <FieldHelp text="Human-readable storage name shown in admin UI." />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">Description</label>
              <Textarea
                value={formData.description}
                onChange={(event) => setFormData((prev) => ({ ...prev, description: event.target.value }))}
                rows={2}
                placeholder="Optional notes about storage purpose and scope."
              />
              <FieldHelp text="Optional documentation for operators." />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">{selectedType.pathLabel}</label>
              <Input
                value={formData.base_directory}
                onChange={(event) => setFormData((prev) => ({ ...prev, base_directory: event.target.value }))}
                disabled={isEditing}
                placeholder={selectedType.pathPlaceholder}
              />
              <FieldHelp text={`${selectedType.pathHelp} Immutable after creation.`} />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">Credentials JSON</label>
              <Textarea
                value={formData.credentials}
                onChange={(event) => setFormData((prev) => ({ ...prev, credentials: event.target.value }))}
                rows={4}
                className="font-mono text-xs"
                disabled={isEditing}
                placeholder='e.g. {"access_key_id":"...","secret_access_key":"..."}'
              />
              <FieldHelp text="Provider credentials payload. Currently set at creation time and treated as immutable." />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">Config JSON</label>
              <Textarea
                value={formData.config_json}
                onChange={(event) => setFormData((prev) => ({ ...prev, config_json: event.target.value }))}
                rows={4}
                className="font-mono text-xs"
                placeholder='e.g. {"region":"us-east-1"}'
              />
              <FieldHelp text="Backend-specific configuration stored in system.storages.config_json." />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">Shared Tables Template</label>
              <Input
                value={formData.shared_tables_template}
                onChange={(event) => setFormData((prev) => ({ ...prev, shared_tables_template: event.target.value }))}
                placeholder="{namespace}/{tableName}/"
              />
              <FieldHelp text="Template for shared table paths. Supports {namespace} and {tableName}." />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">User Tables Template</label>
              <Input
                value={formData.user_tables_template}
                onChange={(event) => setFormData((prev) => ({ ...prev, user_tables_template: event.target.value }))}
                placeholder="{namespace}/{tableName}/{userId}/"
              />
              <FieldHelp text="Template for user table paths. Supports {namespace}, {tableName}, and {userId}." />
            </div>

            {isEditing && storage && (
              <div className="space-y-3 rounded-md border bg-muted/40 p-4">
                <h3 className="text-sm font-semibold">System Fields (Read-only)</h3>
                <div className="grid grid-cols-[140px_1fr] gap-x-3 gap-y-2 text-xs">
                  <span className="text-muted-foreground">Created At</span>
                  <span>{storage.created_at || "N/A"}</span>

                  <span className="text-muted-foreground">Updated At</span>
                  <span>{storage.updated_at || "N/A"}</span>
                </div>
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
          <Button form="storage-form" type="submit" disabled={isSubmitting || !canSubmit}>
            {isSubmitting && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
            {isEditing ? "Save Changes" : "Create Storage"}
          </Button>
        </SheetFooter>
      </SheetContent>
    </Sheet>
  );
}
