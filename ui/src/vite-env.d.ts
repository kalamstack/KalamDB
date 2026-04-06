/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly DEV: boolean;
  readonly PROD: boolean;
  readonly MODE: string;
  readonly VITE_API_URL?: string;
}

interface KalamDbRuntimeConfig {
  readonly backendOrigin?: string;
}

interface Window {
  __KALAMDB_RUNTIME_CONFIG__?: KalamDbRuntimeConfig;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
