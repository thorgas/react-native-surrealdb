import { NativeModules, Platform } from 'react-native';

type StorageConstants = {
  ANDROID_FILES_PATH?: unknown;
  IOS_LIBRARY_PATH?: unknown;
};

const opSQLiteModule = NativeModules.OPSQLite as
  | (StorageConstants & { getConstants?: () => StorageConstants })
  | undefined;
const storageConstants = opSQLiteModule?.getConstants?.() ?? opSQLiteModule;

const storageRoot =
  Platform.OS === 'android'
    ? storageConstants?.ANDROID_FILES_PATH
    : storageConstants?.IOS_LIBRARY_PATH;

if (typeof storageRoot !== 'string' || storageRoot.length === 0) {
  throw new Error('app-private storage root is unavailable');
}

export const syncRestartEndpoint = `surrealkv://${storageRoot.replace(/\/+$/, '')}/surrealdb-sync-restart-e2e`;

export const syncRestartOptions = {
  partitionId: 'restart-partition',
  clientId: 'restart-client',
  requestedScope: 'all',
  subscriptionRevision: 1n,
} as const;

export const syncRestartIdentity = {
  clientCommitId: 'restart-commit',
  fingerprint: 'restart-fingerprint',
} as const;
