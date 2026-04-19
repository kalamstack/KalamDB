// @vitest-environment jsdom

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import LoginForm from "@/components/auth/LoginForm";

const mockLogin = vi.fn();
const mockUseAuth = vi.fn();

vi.mock("@/lib/auth", () => ({
  useAuth: () => mockUseAuth(),
}));

describe("LoginForm", () => {
  afterEach(() => {
    cleanup();
  });

  beforeEach(() => {
    mockLogin.mockReset();
    mockUseAuth.mockReset();
    mockUseAuth.mockReturnValue({
      login: mockLogin,
      error: null,
      isLoading: false,
    });
  });

  it("submits canonical credentials with user + password", async () => {
    render(
      <MemoryRouter>
        <LoginForm />
      </MemoryRouter>,
    );

    fireEvent.change(screen.getByLabelText(/username/i), {
      target: { value: "root" },
    });
    fireEvent.change(screen.getByLabelText(/password/i), {
      target: { value: "kalamdb123" },
    });

    fireEvent.click(screen.getAllByRole("button", { name: /^log in$/i })[0]);

    expect(mockLogin).toHaveBeenCalledTimes(1);
    expect(mockLogin).toHaveBeenCalledWith({
      user: "root",
      password: "kalamdb123",
    });
  });

  it("shows validation error when username or password is missing", () => {
    render(
      <MemoryRouter>
        <LoginForm />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getAllByRole("button", { name: /^log in$/i })[0]);

    expect(screen.getByText(/please enter username and password/i)).toBeTruthy();
    expect(mockLogin).not.toHaveBeenCalled();
  });
});
