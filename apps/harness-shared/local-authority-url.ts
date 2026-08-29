import { NativeModules, Platform } from "react-native";

const LOCAL_AUTHORITY_PORT = 18_091;

type LocalAuthorityRuntime = {
  platform: string;
  scriptUrl?: string;
};

type SourceCodeConstants = {
  getConstants?: () => SourceCodeConstants;
  scriptURL?: unknown;
};

/**
 * Resolves the development authority URL without adding configuration UI.
 * An override wins; otherwise a physical device reuses Metro's host, while
 * Android and iOS loopback use their standard emulator/simulator addresses.
 */
export function resolveLocalAuthorityUrl(
  override?: string,
  runtime: LocalAuthorityRuntime = currentRuntime()
): string {
  if (override !== undefined) return override.replace(/\/+$/, "");

  const metroHost = scriptHost(runtime.scriptUrl);
  const host =
    metroHost && !isLoopback(metroHost)
      ? metroHost
      : runtime.platform === "android"
      ? "10.0.2.2"
      : "127.0.0.1";

  return `http://${host}:${LOCAL_AUTHORITY_PORT}`;
}

function currentRuntime(): LocalAuthorityRuntime {
  const sourceCode = NativeModules.SourceCode as
    | SourceCodeConstants
    | undefined;
  const constants = sourceCode?.getConstants?.() ?? sourceCode;
  return {
    platform: Platform.OS,
    scriptUrl:
      typeof constants?.scriptURL === "string"
        ? constants.scriptURL
        : undefined,
  };
}

function scriptHost(scriptUrl: string | undefined): string | undefined {
  if (!scriptUrl) return undefined;
  try {
    return new URL(scriptUrl).hostname;
  } catch {
    return undefined;
  }
}

function isLoopback(host: string): boolean {
  return (
    host === "localhost" ||
    host === "127.0.0.1" ||
    host === "::1" ||
    host === "[::1]"
  );
}
