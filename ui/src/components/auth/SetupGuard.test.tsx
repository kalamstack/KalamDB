// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { configureStore } from "@reduxjs/toolkit";
import { Provider } from "react-redux";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import SetupGuard from "@/components/auth/SetupGuard";
import setupReducer from "@/store/setupSlice";

const mockCheckSetupStatus = vi.fn(() => ({ type: "setup/checkStatus/mock" }));

vi.mock("@/store/setupSlice", async () => {
  const actual = await vi.importActual<typeof import("@/store/setupSlice")>("@/store/setupSlice");
  return {
    ...actual,
    checkSetupStatus: () => mockCheckSetupStatus(),
  };
});

function renderSetupGuard({
  initialPath,
  setupState,
}: {
  initialPath: string;
  setupState: {
    needsSetup: boolean | null;
    isCheckingStatus: boolean;
    isSubmitting: boolean;
    setupComplete: boolean;
    error: string | null;
    createdUsername: string | null;
  };
}) {
  const store = configureStore({
    reducer: {
      setup: setupReducer,
    },
    preloadedState: {
      setup: setupState,
    },
  });

  return render(
    <Provider store={store}>
      <MemoryRouter initialEntries={[initialPath]}>
        <Routes>
          <Route path="/login" element={<div>Login screen</div>} />
          <Route
            path="/setup"
            element={(
              <SetupGuard>
                <div>Setup wizard</div>
              </SetupGuard>
            )}
          />
          <Route
            path="/dashboard"
            element={(
              <SetupGuard>
                <div>Protected admin content</div>
              </SetupGuard>
            )}
          />
        </Routes>
      </MemoryRouter>
    </Provider>,
  );
}

describe("SetupGuard", () => {
  afterEach(() => {
    cleanup();
    vi.clearAllMocks();
  });

  it("shows the loading state while setup status is being checked", () => {
    renderSetupGuard({
      initialPath: "/dashboard",
      setupState: {
        needsSetup: null,
        isCheckingStatus: true,
        isSubmitting: false,
        setupComplete: false,
        error: null,
        createdUsername: null,
      },
    });

    expect(screen.getByText("Checking server status...")).toBeTruthy();
    expect(mockCheckSetupStatus).toHaveBeenCalledTimes(1);
  });

  it("redirects to the setup page when the server still needs setup", async () => {
    renderSetupGuard({
      initialPath: "/dashboard",
      setupState: {
        needsSetup: true,
        isCheckingStatus: false,
        isSubmitting: false,
        setupComplete: false,
        error: null,
        createdUsername: null,
      },
    });

    expect(await screen.findByText("Setup wizard")).toBeTruthy();
  });

  it("redirects away from the setup page after setup is complete", async () => {
    renderSetupGuard({
      initialPath: "/setup",
      setupState: {
        needsSetup: false,
        isCheckingStatus: false,
        isSubmitting: false,
        setupComplete: false,
        error: null,
        createdUsername: null,
      },
    });

    expect(await screen.findByText("Login screen")).toBeTruthy();
  });

  it("renders children when setup is complete and the user is on an app route", () => {
    renderSetupGuard({
      initialPath: "/dashboard",
      setupState: {
        needsSetup: false,
        isCheckingStatus: false,
        isSubmitting: false,
        setupComplete: false,
        error: null,
        createdUsername: null,
      },
    });

    expect(screen.getByText("Protected admin content")).toBeTruthy();
  });
});
