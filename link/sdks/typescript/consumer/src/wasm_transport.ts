import initConsumerWasm, { KalamConsumerClient as WasmConsumerClient } from '../wasm/kalam_consumer.js';

import type {
  AckResponse,
  ConsumeResponse,
} from './types.js';

type DynamicImport = (specifier: string) => Promise<Record<string, unknown>>;

type NodeProcessShim = {
  versions?: {
    node?: string;
  };
};

type NodeFsPromisesShim = {
  readFile(path: string): Promise<BufferSource>;
};

type NodeUrlShim = {
  fileURLToPath(url: URL): string;
};

const dynamicImport = new Function(
  'specifier',
  'return import(specifier)',
) as DynamicImport;

let sharedConsumerWasmInit: Promise<void> | null = null;

async function initializeConsumerWasmModule(wasmUrl?: string | BufferSource): Promise<void> {
  if (!sharedConsumerWasmInit) {
    sharedConsumerWasmInit = (async () => {
      if (wasmUrl) {
        await initConsumerWasm({ module_or_path: wasmUrl });
      } else {
        await initConsumerWasm();
      }
    })().catch((error) => {
      sharedConsumerWasmInit = null;
      throw error;
    });
  }

  await sharedConsumerWasmInit;
}

function getNodeProcess(): NodeProcessShim | undefined {
  const runtime = globalThis as typeof globalThis & {
    process?: NodeProcessShim;
  };
  return runtime.process;
}

export class ConsumerWasmTransport {
  private wasmClient: WasmConsumerClient | null = null;
  private initialized = false;
  private initializing: Promise<void> | null = null;
  private wasmUrl?: string | BufferSource;

  constructor(
    private readonly url: string,
    wasmUrl?: string | BufferSource,
  ) {
    this.wasmUrl = wasmUrl;
  }

  async consume(
    authHeader: string | undefined,
    request: unknown,
  ): Promise<ConsumeResponse> {
    await this.initialize();
    return this.wasmClient!.consume(authHeader, request) as Promise<ConsumeResponse>;
  }

  async ack(
    authHeader: string | undefined,
    request: unknown,
  ): Promise<AckResponse> {
    await this.initialize();
    return this.wasmClient!.ack(authHeader, request) as Promise<AckResponse>;
  }

  private async initialize(): Promise<void> {
    if (this.initialized) {
      return;
    }

    if (this.initializing) {
      await this.initializing;
      return;
    }

    this.initializing = (async () => {
      await this.ensureNodeRuntimeCompat();
      await initializeConsumerWasmModule(this.wasmUrl);
      this.wasmClient = new WasmConsumerClient(this.url);
      this.initialized = true;
    })().catch((error) => {
      this.initializing = null;
      throw error;
    });

    await this.initializing;
  }

  private async ensureNodeRuntimeCompat(): Promise<void> {
    const isNodeRuntime = Boolean(getNodeProcess()?.versions?.node);
    if (!isNodeRuntime) {
      return;
    }

    if (typeof globalThis.fetch !== 'function') {
      throw new Error('Node.js runtime is missing fetch() support. Node.js 18+ is required.');
    }

    if (!this.wasmUrl) {
      try {
        const [{ readFile }, { fileURLToPath }] = await Promise.all([
          dynamicImport('node:fs/promises') as Promise<NodeFsPromisesShim>,
          dynamicImport('node:url') as Promise<NodeUrlShim>,
        ]);
        const wasmFileUrl = new URL('../wasm/kalam_consumer_bg.wasm', import.meta.url);
        this.wasmUrl = await readFile(fileURLToPath(wasmFileUrl));
      } catch (error) {
        throw new Error(
          `Node.js runtime could not load bundled consumer WASM file. Build the SDK and ensure dist/wasm/kalam_consumer_bg.wasm exists. Cause: ${error}`,
        );
      }
    }
  }
}