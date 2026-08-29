import { afterEach, describe, expect, it, vi } from "vitest";

import { ExperimentalSyncWebSocketHints } from "../src/sync-websocket-hints";

class FakeSocket {
  onopen: ((event: Event) => void) | null = null;
  onmessage: ((event: MessageEvent) => void) | null = null;
  onerror: ((event: Event) => void) | null = null;
  onclose: ((event: CloseEvent) => void) | null = null;
  readonly close = vi.fn((code?: number, reason?: string) => {
    this.onclose?.({ code, reason } as CloseEvent);
  });

  open() {
    this.onopen?.({} as Event);
  }

  message(data: unknown) {
    this.onmessage?.({ data } as MessageEvent);
  }
}

afterEach(() => vi.useRealTimers());

describe("ExperimentalSyncWebSocketHints", () => {
  it("turns bounded messages into coalesced pull hints without decoding payloads", async () => {
    let current = 1_000;
    const socket = new FakeSocket();
    const onHint = vi.fn();
    const onFailure = vi.fn();
    const hints = new ExperimentalSyncWebSocketHints({
      url: () => "wss://sync.example.test/hints?ticket=redacted",
      webSocketFactory: () => socket as unknown as WebSocket,
      minimumHintIntervalMs: 100,
      now: () => current,
    });

    const stop = hints.start(onHint, onFailure);
    await vi.waitFor(() => expect(socket.onmessage).not.toBeNull());
    socket.message('{"record":"must-not-be-decoded"}');
    socket.message("duplicate");
    expect(onHint).toHaveBeenCalledOnce();
    current += 100;
    socket.message(new Uint8Array([1, 2, 3]));
    expect(onHint).toHaveBeenCalledTimes(2);
    expect(onFailure).not.toHaveBeenCalled();
    stop();
  });

  it("closes oversized or unsupported messages fail closed", async () => {
    const socket = new FakeSocket();
    const onFailure = vi.fn();
    const hints = new ExperimentalSyncWebSocketHints({
      url: () => "ws://127.0.0.1/hints",
      allowInsecureLocalhost: true,
      webSocketFactory: () => socket as unknown as WebSocket,
      maxMessageBytes: 4,
    });
    const stop = hints.start(vi.fn(), onFailure);
    await vi.waitFor(() => expect(socket.onmessage).not.toBeNull());

    socket.message("12345");
    expect(onFailure).toHaveBeenCalledOnce();
    expect(socket.close).toHaveBeenCalledWith(1009, "hint too large");
    stop();
  });

  it("refreshes the ticket on reconnect and permanently stops", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const url = vi
      .fn<() => Promise<string>>()
      .mockResolvedValueOnce("wss://sync.example.test/hints?ticket=one")
      .mockResolvedValueOnce("wss://sync.example.test/hints?ticket=two");
    const factory = vi.fn((_: string) => {
      const socket = new FakeSocket();
      sockets.push(socket);
      return socket as unknown as WebSocket;
    });
    const hints = new ExperimentalSyncWebSocketHints({
      url,
      webSocketFactory: factory,
      baseReconnectMs: 100,
      maxReconnectMs: 100,
      random: () => 0.5,
    });
    const stop = hints.start(vi.fn(), vi.fn());
    await Promise.resolve();
    expect(factory).toHaveBeenCalledWith(
      "wss://sync.example.test/hints?ticket=one",
    );
    sockets[0]?.open();
    sockets[0]?.close(1006, "lost");
    await vi.advanceTimersByTimeAsync(50);
    expect(factory).toHaveBeenCalledWith(
      "wss://sync.example.test/hints?ticket=two",
    );
    stop();
    expect(sockets[1]?.close).toHaveBeenCalledWith(1000, "sync hints stopped");
    await vi.runAllTimersAsync();
    expect(factory).toHaveBeenCalledTimes(2);
  });

  it("requires secure transport unless localhost is explicitly allowed", async () => {
    const onFailure = vi.fn();
    const factory = vi.fn(() => new FakeSocket() as unknown as WebSocket);
    const hints = new ExperimentalSyncWebSocketHints({
      url: () => "ws://sync.example.test/hints?ticket=redacted",
      webSocketFactory: factory,
    });
    const stop = hints.start(vi.fn(), onFailure);
    await vi.waitFor(() => expect(onFailure).toHaveBeenCalledOnce());
    expect(factory).not.toHaveBeenCalled();
    stop();
  });
});
