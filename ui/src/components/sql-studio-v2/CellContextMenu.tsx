import { useEffect, useRef } from "react";
import { Copy, Eye, Pencil, Trash2, Undo2 } from "lucide-react";
import { cn } from "@/lib/utils";

export interface CellContextMenuState {
  x: number;
  y: number;
  rowIndex: number;
  columnName: string;
  value: unknown;
  rowStatus: "deleted" | "edited" | null;
  cellEdited: boolean;
}

interface CellContextMenuProps {
  context: CellContextMenuState | null;
  onEdit: (rowIndex: number, columnName: string, currentValue: unknown) => void;
  onDelete: (rowIndex: number) => void;
  onUndoEdit: (rowIndex: number) => void;
  onUndoDelete: (rowIndex: number) => void;
  onCopyValue: (value: unknown) => void;
  onViewData: (value: unknown) => void;
  onClose: () => void;
}

export function CellContextMenu({
  context,
  onEdit,
  onDelete,
  onUndoEdit,
  onUndoDelete,
  onCopyValue,
  onViewData,
  onClose,
}: CellContextMenuProps) {
  const menuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!context) return;
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
    };
    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [context, onClose]);

  useEffect(() => {
    if (!context || !menuRef.current) return;
    const rect = menuRef.current.getBoundingClientRect();
    const viewportWidth = window.innerWidth;
    const viewportHeight = window.innerHeight;

    if (rect.right > viewportWidth) {
      menuRef.current.style.left = `${context.x - rect.width}px`;
    }
    if (rect.bottom > viewportHeight) {
      menuRef.current.style.top = `${context.y - rect.height}px`;
    }
  }, [context]);

  if (!context) return null;

  const isDeleted = context.rowStatus === "deleted";
  const isEdited = context.rowStatus === "edited";
  const hasViewableData =
    context.value !== null &&
    (typeof context.value === "object" ||
      (typeof context.value === "string" && context.value.length > 40));
  const itemClassName =
    "relative flex w-full cursor-default select-none items-center gap-2 rounded-sm px-2 py-1.5 text-left text-sm outline-none transition-colors hover:bg-accent hover:text-accent-foreground disabled:pointer-events-none disabled:opacity-50";

  return (
    <>
      <div className="fixed inset-0 z-40" onClick={onClose} />

      <div
        ref={menuRef}
        className="fixed z-50 min-w-[11rem] overflow-hidden rounded-md border bg-popover p-1 text-popover-foreground opacity-100 shadow-md antialiased backdrop-blur-none"
        style={{ left: context.x, top: context.y }}
      >
        <div className="px-2 py-1.5 text-xs font-medium text-muted-foreground">
          {context.columnName} · Row {context.rowIndex + 1}
        </div>
        <div className="-mx-1 my-1 h-px bg-muted" />

        {!isDeleted && (
          <button
            className={itemClassName}
            onClick={() => {
              onEdit(context.rowIndex, context.columnName, context.value);
              onClose();
            }}
          >
            <Pencil className="h-3.5 w-3.5 text-foreground/80" />
            Edit Cell
          </button>
        )}

        {!isDeleted && (
          <button
            className={cn(itemClassName, "text-destructive hover:bg-destructive/10 hover:text-destructive")}
            onClick={() => {
              onDelete(context.rowIndex);
              onClose();
            }}
          >
            <Trash2 className="h-3.5 w-3.5" />
            Delete Row
          </button>
        )}

        {isDeleted && (
          <button
            className={itemClassName}
            onClick={() => {
              onUndoDelete(context.rowIndex);
              onClose();
            }}
          >
            <Undo2 className="h-3.5 w-3.5" />
            Undo Delete
          </button>
        )}

        {isEdited && !isDeleted && (
          <button
            className={itemClassName}
            onClick={() => {
              onUndoEdit(context.rowIndex);
              onClose();
            }}
          >
            <Undo2 className="h-3.5 w-3.5" />
            Undo Row Edits
          </button>
        )}

        <div className="-mx-1 my-1 h-px bg-muted" />

        <button
          className={itemClassName}
          disabled={!hasViewableData}
          onClick={() => {
            if (!hasViewableData) {
              return;
            }
            onViewData(context.value);
            onClose();
          }}
        >
          <Eye className="h-3.5 w-3.5" />
          View Data
        </button>

        <button
          className={itemClassName}
          onClick={() => {
            onCopyValue(context.value);
            onClose();
          }}
        >
          <Copy className="h-3.5 w-3.5" />
          Copy Value
        </button>
      </div>
    </>
  );
}