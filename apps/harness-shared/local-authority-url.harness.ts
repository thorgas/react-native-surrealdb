import { describe, expect, test } from "react-native-harness";

import { resolveLocalAuthorityUrl } from "./local-authority-url";

describe("local authority URL", () => {
  test("uses an explicit override before device detection", () => {
    expect(
      resolveLocalAuthorityUrl("http://authority.test:19090/", {
        platform: "android",
        scriptUrl: "http://192.168.1.8:8081/index.bundle",
      })
    ).toBe("http://authority.test:19090");
  });

  test("uses the Metro host for a physical device", () => {
    expect(
      resolveLocalAuthorityUrl(undefined, {
        platform: "ios",
        scriptUrl: "http://192.168.1.8:8081/index.bundle?platform=ios",
      })
    ).toBe("http://192.168.1.8:18091");
  });

  test("uses the Android emulator host for a loopback Metro URL", () => {
    expect(
      resolveLocalAuthorityUrl(undefined, {
        platform: "android",
        scriptUrl: "http://localhost:8081/index.bundle?platform=android",
      })
    ).toBe("http://10.0.2.2:18091");
  });

  test("uses iOS loopback when Metro has no reachable device host", () => {
    expect(
      resolveLocalAuthorityUrl(undefined, {
        platform: "ios",
        scriptUrl: "file:///main.jsbundle",
      })
    ).toBe("http://127.0.0.1:18091");
  });
});
