import { cn } from "@/lib/utils";
import { ScrollArea, ScrollBar } from "@/components/ui/scroll-area";
import { KalamCellValue } from "kalam-link";

interface CodeBlockProps {
  value: unknown;
  className?: string;
  maxHeightClassName?: string;
  jsonPreferred?: boolean;
}

interface NormalizedCode {
  text: string;
  isJson: boolean;
}

function unwrapSerializableValue(value: unknown): unknown {
  if (value instanceof KalamCellValue) {
    return unwrapSerializableValue(value.toJson());
  }

  if (Array.isArray(value)) {
    return value.map((item) => unwrapSerializableValue(item));
  }

  if (value && typeof value === "object") {
    const maybeSerializable = value as { toJson?: () => unknown };
    if (typeof maybeSerializable.toJson === "function") {
      try {
        return unwrapSerializableValue(maybeSerializable.toJson());
      } catch {
        // Fall through to entry-wise unwrap.
      }
    }

    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, entry]) => [
        key,
        unwrapSerializableValue(entry),
      ]),
    );
  }

  return value;
}

function normalizeCode(value: unknown, jsonPreferred: boolean): NormalizedCode {
  const raw = unwrapSerializableValue(value);

  if (raw === null || raw === undefined) {
    return { text: "null", isJson: true };
  }

  if (typeof raw === "string") {
    const trimmed = raw.trim();
    const maybeJson =
      jsonPreferred ||
      ((trimmed.startsWith("{") && trimmed.endsWith("}")) ||
        (trimmed.startsWith("[") && trimmed.endsWith("]")));

    if (maybeJson) {
      try {
        const parsed = JSON.parse(raw);
        return { text: JSON.stringify(parsed, null, 2), isJson: true };
      } catch {
        return { text: raw, isJson: false };
      }
    }
    return { text: raw, isJson: false };
  }

  if (typeof raw === "object") {
    try {
      return { text: JSON.stringify(raw, null, 2), isJson: true };
    } catch {
      return { text: String(raw), isJson: false };
    }
  }

  return { text: String(raw), isJson: false };
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function highlightJson(json: string): string {
  const escaped = escapeHtml(json);
  return escaped.replace(
    /("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(\.\d+)?([eE][+\-]?\d+)?)/g,
    (match) => {
      if (/^".*":$/.test(match)) {
        return `<span class="text-sky-400">${match}</span>`;
      }
      if (/^"/.test(match)) {
        return `<span class="text-emerald-400">${match}</span>`;
      }
      if (/true|false/.test(match)) {
        return `<span class="text-violet-400">${match}</span>`;
      }
      if (/null/.test(match)) {
        return `<span class="text-slate-500 italic">${match}</span>`;
      }
      return `<span class="text-amber-400">${match}</span>`;
    },
  );
}

export function CodeBlock({
  value,
  className,
  maxHeightClassName = "max-h-[60vh]",
  jsonPreferred = false,
}: CodeBlockProps) {
  const normalized = normalizeCode(value, jsonPreferred);
  const highlighted = normalized.isJson ? highlightJson(normalized.text) : null;

  return (
    <div className={cn("flex h-full min-h-0 flex-col rounded-md border border-slate-700 bg-black", className)}>
      <ScrollArea className={cn("min-h-0 w-full flex-1", maxHeightClassName)}>
        {normalized.isJson && highlighted ? (
          <pre className="whitespace-pre p-3 font-mono text-xs leading-5 text-slate-200">
            <code dangerouslySetInnerHTML={{ __html: highlighted }} />
          </pre>
        ) : (
          <pre className="whitespace-pre-wrap p-3 font-mono text-xs leading-5 text-slate-200">
            {normalized.text}
          </pre>
        )}
        <ScrollBar orientation="vertical" />
        <ScrollBar orientation="horizontal" />
      </ScrollArea>
    </div>
  );
}
