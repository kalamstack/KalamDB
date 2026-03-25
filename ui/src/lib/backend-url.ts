function trimTrailingSlashes(value: string): string {
  return value.replace(/\/+$/, "");
}

function resolveConfiguredOrigin(): string | null {
  const configured = import.meta.env.VITE_API_URL?.trim();
  if (!configured) {
    return null;
  }

  return trimTrailingSlashes(configured);
}

export function getBackendOrigin(): string {
  const configuredOrigin = resolveConfiguredOrigin();
  if (configuredOrigin) {
    return configuredOrigin;
  }

  if (typeof window !== "undefined") {
    return trimTrailingSlashes(window.location.origin);
  }

  return "http://localhost:8080";
}

export function getApiBaseUrl(): string {
  return `${getBackendOrigin()}/v1/api`;
}
