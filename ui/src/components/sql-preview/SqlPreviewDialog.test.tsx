// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { SqlPreviewDialog } from "./SqlPreviewDialog";

const monacoEditorState = vi.hoisted(() => ({
  setValue: vi.fn(),
}));

vi.mock("@monaco-editor/react", () => ({
  default: ({
    value,
    onChange,
    onMount,
    options,
  }: {
    value?: string;
    onChange?: (value: string) => void;
    onMount?: (editor: { setValue: (nextValue: string) => void }) => void;
    options?: { readOnly?: boolean };
  }) => {
    onMount?.({ setValue: monacoEditorState.setValue });

    return (
      <textarea
        aria-label="SQL preview editor"
        readOnly={Boolean(options?.readOnly)}
        value={value ?? ""}
        onChange={(event) => onChange?.(event.target.value)}
      />
    );
  },
}));

vi.mock("@/components/ui/resizable", () => ({
  ResizablePanelGroup: ({ children, className }: { children: React.ReactNode; className?: string }) => (
    <div className={className}>{children}</div>
  ),
  ResizablePanel: ({ children, className }: { children: React.ReactNode; className?: string }) => (
    <div className={className}>{children}</div>
  ),
  ResizableHandle: ({ className }: { className?: string }) => <div className={className} aria-hidden="true" />,
}));

describe("SqlPreviewDialog", () => {
  it("renders the review SQL in a read-only Monaco surface and executes the batch once", async () => {
    const sql = [
      "BEGIN;",
      "UPDATE public.users SET name = 'Grace' WHERE id = 1;",
      "DELETE FROM public.users WHERE id = 2;",
      "COMMIT;",
    ].join("\n");
    const onExecute = vi.fn().mockResolvedValue(undefined);

    render(
      <SqlPreviewDialog
        open
        onClose={vi.fn()}
        options={{
          title: "Review Changes",
          sql,
          statements: [
            "BEGIN;",
            "UPDATE public.users SET name = 'Grace' WHERE id = 1;",
            "DELETE FROM public.users WHERE id = 2;",
            "COMMIT;",
          ],
          editable: false,
          onExecute,
        }}
      />,
    );

    const editor = screen.getByLabelText("SQL preview editor") as HTMLTextAreaElement;
    expect(editor.readOnly).toBe(true);
    expect(editor.value).toBe(sql);

    fireEvent.click(screen.getByRole("button", { name: /commit/i }));

    await waitFor(() => expect(onExecute).toHaveBeenCalledTimes(1));
    expect(onExecute).toHaveBeenCalledWith(sql);
    await waitFor(() => {
      expect(screen.getByText(/transaction committed across 2 change statements/i)).toBeInTheDocument();
    });
  });
});