// @vitest-environment jsdom

import { render, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const {
  mockUseAuth,
  mockProbeBackendReachability,
  mockConnectWebSocket,
  mockIsSubscriptionClientConnected,
  mockSubscribeToClientConnectivity,
} = vi.hoisted(() => ({
  mockUseAuth: vi.fn(),
  mockProbeBackendReachability: vi.fn(),
  mockConnectWebSocket: vi.fn(),
  mockIsSubscriptionClientConnected: vi.fn(),
  mockSubscribeToClientConnectivity: vi.fn(),
}));

vi.mock("./auth", () => ({
  useAuth: () => mockUseAuth(),
}));

vi.mock("./api", () => ({
  probeBackendReachability: mockProbeBackendReachability,
}));

vi.mock("./backend-url", () => ({
  getApiBaseUrl: () => "http://localhost:8080/v1/api",
  getBackendOrigin: () => "http://localhost:8080",
}));

vi.mock("./kalam-client", () => ({
  connectWebSocket: mockConnectWebSocket,
  isSubscriptionClientConnected: mockIsSubscriptionClientConnected,
  subscribeToClientConnectivity: mockSubscribeToClientConnectivity,
}));

import { BackendStatusProvider } from "./backend-status";

describe("BackendStatusProvider", () => {
  beforeEach(() => {
    mockUseAuth.mockReturnValue({
      isAuthenticated: true,
      accessToken: "jwt-token",
      isLoading: false,
    });
    mockProbeBackendReachability.mockResolvedValue(undefined);
    mockConnectWebSocket.mockResolvedValue(undefined);
    mockIsSubscriptionClientConnected.mockReturnValue(false);
    mockSubscribeToClientConnectivity.mockReturnValue(() => {});

    Object.defineProperty(document, "visibilityState", {
      configurable: true,
      value: "visible",
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("reconnects the websocket when the tab becomes visible again", async () => {
    render(
      <BackendStatusProvider>
        <div>status</div>
      </BackendStatusProvider>,
    );

    await waitFor(() => {
      expect(mockConnectWebSocket).toHaveBeenCalledTimes(1);
    });

    Object.defineProperty(document, "visibilityState", {
      configurable: true,
      value: "hidden",
    });
    document.dispatchEvent(new Event("visibilitychange"));

    expect(mockConnectWebSocket).toHaveBeenCalledTimes(1);

    Object.defineProperty(document, "visibilityState", {
      configurable: true,
      value: "visible",
    });
    document.dispatchEvent(new Event("visibilitychange"));

    await waitFor(() => {
      expect(mockConnectWebSocket).toHaveBeenCalledTimes(2);
    });
  });
});