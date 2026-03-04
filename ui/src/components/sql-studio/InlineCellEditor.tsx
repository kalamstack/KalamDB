/**
 * InlineCellEditor - A small floating input that appears when the user
 * chooses "Edit Cell" from the context menu.
 *
 * Supports editing text, numbers, booleans, and NULL values.
 * Press Enter or click away to confirm; press Escape to cancel.
 *
 * Usage:
 *   <InlineCellEditor
 *     context={{ rowIndex: 0, columnName: 'name', value: 'Alice', rect: { ... } }}
 *     onSave={(rowIndex, columnName, oldValue, newValue) => { ... }}
 *     onCancel={() => setEditContext(null)}
 *   />
 */

import { useState, useRef, useEffect, useCallback } from 'react';
import { Button } from '@/components/ui/button';
import { Check, X } from 'lucide-react';
import { KalamCellValue } from 'kalam-link';

// ─── Types ───────────────────────────────────────────────────────────────────

export interface InlineEditContext {
  rowIndex: number;
  columnName: string;
  value: unknown;
  /** Bounding rect of the cell, used to position the editor. */
  rect: DOMRect;
}

interface InlineCellEditorProps {
  context: InlineEditContext | null;
  onSave: (rowIndex: number, columnName: string, oldValue: unknown, newValue: unknown) => void;
  onCancel: () => void;
}

// ─── Component ───────────────────────────────────────────────────────────────

export function InlineCellEditor({ context, onSave, onCancel }: InlineCellEditorProps) {
  const [editValue, setEditValue] = useState('');
  const [isNull, setIsNull] = useState(false);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  // Initialize the edit value when the context changes
  useEffect(() => {
    if (!context) return;
    // Unwrap KalamCellValue to get the raw underlying value
    const raw = context.value instanceof KalamCellValue ? context.value.toJson() : context.value;
    if (raw === null || raw === undefined) {
      setEditValue('');
      setIsNull(true);
    } else if (typeof raw === 'object') {
      setEditValue(JSON.stringify(raw, null, 2));
      setIsNull(false);
    } else {
      setEditValue(String(raw));
      setIsNull(false);
    }
    // Focus the input after a tick
    setTimeout(() => inputRef.current?.focus(), 50);
  }, [context]);

  const handleSave = useCallback(() => {
    if (!context) return;

    let newValue: unknown;
    if (isNull) {
      newValue = null;
    } else {
      // Try to parse back to the original type (unwrap KalamCellValue if needed)
      const original = context.value instanceof KalamCellValue ? context.value.toJson() : context.value;
      if (typeof original === 'number') {
        const parsed = Number(editValue);
        newValue = isNaN(parsed) ? editValue : parsed;
      } else if (typeof original === 'boolean') {
        newValue = editValue.toLowerCase() === 'true';
      } else if (typeof original === 'object' && original !== null) {
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
    (e: React.KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.preventDefault();
        onCancel();
      }
      // Ctrl/Cmd + Enter to save
      if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
        e.preventDefault();
        handleSave();
      }
    },
    [onCancel, handleSave],
  );

  if (!context) return null;

  return (
    <>
      {/* Backdrop */}
      <div className="fixed inset-0 z-40 bg-black/10" onClick={handleSave} />

      {/* Editor overlay */}
      <div
        className="fixed z-50 bg-background border-2 border-blue-500 rounded-md shadow-xl p-2 min-w-[200px] max-w-[400px]"
        style={{
          left: Math.min(context.rect.left, window.innerWidth - 420),
          top: Math.min(context.rect.top, window.innerHeight - 200),
        }}
      >
        {/* Column label */}
        <div className="text-xs text-muted-foreground mb-1 font-mono">
          {context.columnName}
        </div>

        {/* Text input */}
        <textarea
          ref={inputRef}
          value={isNull ? '' : editValue}
          onChange={(e) => {
            setEditValue(e.target.value);
            setIsNull(false);
          }}
          onKeyDown={handleKeyDown}
          disabled={isNull}
          placeholder={isNull ? 'NULL' : 'Enter value...'}
          className="w-full min-h-[60px] max-h-[200px] px-2 py-1.5 text-sm font-mono border rounded resize-y bg-background disabled:opacity-50 disabled:bg-muted focus:outline-none focus:ring-2 focus:ring-blue-500"
          rows={typeof context.value === 'object' ? 4 : 2}
        />

        {/* Controls */}
        <div className="flex items-center justify-between mt-2">
          <label className="flex items-center gap-1.5 text-xs text-muted-foreground cursor-pointer">
            <input
              type="checkbox"
              checked={isNull}
              onChange={(e) => setIsNull(e.target.checked)}
              className="rounded border-gray-300"
            />
            Set NULL
          </label>
          <div className="flex items-center gap-1">
            <Button size="sm" variant="ghost" onClick={onCancel} className="h-7 px-2 text-xs gap-1">
              <X className="h-3 w-3" />
              Cancel
            </Button>
            <Button size="sm" onClick={handleSave} className="h-7 px-2 text-xs gap-1">
              <Check className="h-3 w-3" />
              Save
            </Button>
          </div>
        </div>
        <div className="text-[10px] text-muted-foreground mt-1">
          Ctrl+Enter to save · Esc to cancel
        </div>
      </div>
    </>
  );
}
