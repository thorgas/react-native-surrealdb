import { NativeModules, Platform } from "react-native";

type StorageConstants = {
  ANDROID_FILES_PATH?: unknown;
  IOS_LIBRARY_PATH?: unknown;
};

const opSQLiteModule = NativeModules.OPSQLite as
  | (StorageConstants & { getConstants?: () => StorageConstants })
  | undefined;
const storageConstants = opSQLiteModule?.getConstants?.() ?? opSQLiteModule;
const storageRoot =
  Platform.OS === "android"
    ? storageConstants?.ANDROID_FILES_PATH
    : storageConstants?.IOS_LIBRARY_PATH;

if (typeof storageRoot !== "string" || storageRoot.length === 0) {
  throw new Error("app-private storage root is unavailable");
}

export const surrealkvChurnEndpoint = `surrealkv://${storageRoot.replace(
  /\/+$/,
  ""
)}/surrealkv-churn-e2e`;
export const surrealkvChurnIterations = 64;
