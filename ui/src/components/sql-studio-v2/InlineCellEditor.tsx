import { useCallback, useEffect, useRef, useState } from "react";
import { Check, X } from "lucide-react";
import { KalamCellValue } from "@kalamdb/client";
import { Button } from "@/components/ui/button";

export interface InlineEditContext {
  rowIndex: number;
  columnName: string;
  value: unknown;
  rect: DOMRect;
}

interface InlineCellEditorProps {
  context: InlineEditContext | null;
  onSave: (rowIndex: number, columnName: string, oldValue: unknown, newValue: unknown) => void;
  onCancel: () => void;
}

export function InlineCellEditor({ context, onSave, onCancel }: InlineCellEditorProps) {
  const [editValue, setEditValue] = useState("");
  const [isNull, setIsNull] = useState(false);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    if (!context) return;
    const raw = context.value instanceof KalamCellValue ? context.value.toJson() : context.value;
    if (raw === null || raw === undefined) {
      setEditValue("");
      setIsNull(true);
    } else if (typeof raw === "object") {
      setEditValue(JSON.stringify(raw, null, 2));
      setIsNull(false);
    } else {
      setEditValue(String(raw));
      setIsNull(false);
    }
    setTimeout(() => inputRef.current?.focus(), 50);
  }, [context]);

  const handleSave = useCallback(() => {
    if (!context) return;

    let newValue: unknown;
    if (isNull) {
      newValue = null;
    } else {
      const original = context.value instanceof KalamCellValue ? context.value.toJson() : context.value;
      if (typeof original === "number") {
        const parsed = Number(editValue);
        newValue = Number.isNaN(parsed) ? editValue : parsed;
      } else if (typeof original === "boolean") {
        newValue = editValue.toLowerCase() === "true";
      } else if (typeof original === "object" && original !== null) {
        try {
          newValue = JSON.parse(editValue);
        } catch {
          newValue = editValue;
        }
      } else {
        newValue = editValue;
      }
    }

    onSave(context.rowIndex, context.columnName, context.value, newValue);
  }, [context, editValue, isNull, onSave]);

  const handleKeyDown = useCallback(
    (event: React.KeyboardEvent) => {
      if (event.key === "Escape") {
        event.preventDefault();
        onCancel();
      }
      if (event.key === "Enter" && (event.ctrlKey || event.metaKey)) {
        event.preventDefault();
        handleSave();
      }
    },
    [handleSave, onCancel],
  );

  if (!context) return null;

  return (
    <>
      <div className="fixed inset-0 z-40 bg-black/10" onClick={handleSave} />

      <div
        className="fixed z-50 min-w-[200px] max-w-[400px] rounded-md border-2 border-blue-500 bg-background p-2 shadow-xl"
        style={{
          left: Math.min(context.rect.left, window.innerWidth - 420),
          top: Math.min(context.rect.top, window.innerHeight - 200),
        }}
      >
        <div className="mb-1 text-xs font-mono text-muted-foreground">{context.columnName}</div>

        <textarea
          ref={inputRef}
          value={isNull ? "" : editValue}
          onChange={(event) => {
            setEditValue(event.target.value);
            setIsNull(false);
          }}
          onKeyDown={handleKeyDown}
          disabled={isNull}
          placeholder={isNull ? "NULL" : "Enter value..."}
          className="min-h-[60px] max-h-[200px] w-full resize-y rounded border bg-background px-2 py-1.5 font-mono text-sm disabled:bg-muted disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
          rows={typeof context.value === "object" ? 4 : 2}
        />

        <div className="mt-2 flex items-center justify-between">
          <label className="flex cursor-pointer items-center gap-1.5 text-xs text-muted-foreground">
            <input
              type="checkbox"
              checked={isNull}
              onChange={(event) => setIsNull(event.target.checked)}
              className="rounded border-gray-300"
            />
            Set NULL
          </label>
          <div className="flex items-center gap-1">
            <Button size="sm" variant="ghost" onClick={onCancel} className="h-7 gap-1 px-2 text-xs">
              <X className="h-3 w-3" />
              Cancel
            </Button>
            <Button size="sm" onClick={handleSave} className="h-7 gap-1 px-2 text-xs">
              <Check className="h-3 w-3" />
              Save
            </Button>
          </div>
        </div>
        <div className="mt-1 text-[10px] text-muted-foreground">Ctrl+Enter to save · Esc to cancel</div>
      </div>
    </>
  );
}