/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_CHAT_ROOM: string
  readonly VITE_KALAMDB_URL: string
  readonly VITE_KALAMDB_USER: string
  readonly VITE_KALAMDB_PASSWORD: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}