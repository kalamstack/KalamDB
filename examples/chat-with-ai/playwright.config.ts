import { defineConfig } from '@playwright/test';

const chatTestRoom = process.env.CHAT_TEST_ROOM ?? `playwright-room-${Date.now()}`;
process.env.CHAT_TEST_ROOM = chatTestRoom;

export default defineConfig({
  testDir: './tests',
  timeout: 90_000,
  fullyParallel: false,
  use: {
    baseURL: 'http://127.0.0.1:5174',
    headless: true,
  },
  webServer: {
    command: 'npm run dev -- --host 127.0.0.1 --port 5174',
    env: {
      ...process.env,
      VITE_CHAT_ROOM: chatTestRoom,
    },
    port: 5174,
    reuseExistingServer: false,
    timeout: 60_000,
  },
});