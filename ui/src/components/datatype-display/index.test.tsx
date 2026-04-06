// @vitest-environment jsdom

import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { CellDisplay } from "./index";

vi.mock("@kalamdb/client", () => {
  class KalamCellValue {
    private value: unknown;

    constructor(value: unknown) {
      this.value = value;
    }

    static from(value: unknown) {
      return new KalamCellValue(value);
    }

    toJson() {
      return this.value;
    }
  }

  return { KalamCellValue };
});

describe("CellDisplay", () => {
  it("renders bigint-like strings without losing precision", () => {
    render(
      <CellDisplay
        value="9223372036854775807"
        dataType="BIGINT"
      />,
    );

    expect(screen.getByText("9223372036854775807")).toBeTruthy();
  });
});
