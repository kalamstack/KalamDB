// @vitest-environment jsdom

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import SetupWizard from "@/pages/SetupWizard";

const mockDispatch = vi.fn();
const mockSubmitSetup = vi.fn();
const mockClearSetupError = vi.fn();

let setupState = {
  needsSetup: true,
  isCheckingStatus: false,
  isSubmitting: false,
  setupComplete: false,
  error: null as string | null,
  createdUsername: null as string | null,
};

vi.mock("@/store/hooks", () => ({
  useAppDispatch: () => mockDispatch,
  useAppSelector: (selector: (state: { setup: typeof setupState }) => unknown) =>
    selector({ setup: setupState }),
}));

vi.mock("@/store/setupSlice", () => ({
  clearSetupError: () => {
    mockClearSetupError();
    return { type: "setup/clearSetupError" };
  },
  submitSetup: (payload: unknown) => {
    mockSubmitSetup(payload);
    return { type: "setup/submit", payload };
  },
}));

describe("SetupWizard", () => {
  beforeEach(() => {
    mockDispatch.mockReset();
    mockSubmitSetup.mockReset();
    mockClearSetupError.mockReset();
    setupState = {
      needsSetup: true,
      isCheckingStatus: false,
      isSubmitting: false,
      setupComplete: false,
      error: null,
      createdUsername: null,
    };
  });

  it("submits canonical setup payload after completing the wizard", () => {
    render(
      <MemoryRouter>
        <SetupWizard />
      </MemoryRouter>,
    );

    fireEvent.change(screen.getByLabelText(/username/i), {
      target: { value: "admin_user" },
    });
    fireEvent.change(screen.getByLabelText(/email/i), {
      target: { value: "admin@example.com" },
    });
    fireEvent.click(screen.getByRole("button", { name: /continue/i }));

    fireEvent.change(screen.getByLabelText(/^password/i), {
      target: { value: "StrongPass123!" },
    });
    fireEvent.change(screen.getByLabelText(/confirm password/i), {
      target: { value: "StrongPass123!" },
    });
    fireEvent.click(screen.getByRole("button", { name: /continue/i }));

    fireEvent.change(screen.getByLabelText(/^root password/i), {
      target: { value: "RootPass123!" },
    });
    fireEvent.change(screen.getByLabelText(/confirm root password/i), {
      target: { value: "RootPass123!" },
    });
    fireEvent.click(screen.getByRole("button", { name: /complete setup/i }));

    expect(mockSubmitSetup).toHaveBeenCalledTimes(1);
    expect(mockSubmitSetup).toHaveBeenCalledWith({
      user: "admin_user",
      password: "StrongPass123!",
      root_password: "RootPass123!",
      email: "admin@example.com",
    });
    expect(mockDispatch).toHaveBeenCalledTimes(1);
  });
});
