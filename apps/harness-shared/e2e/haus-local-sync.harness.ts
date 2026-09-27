import { describe, expect, test } from "react-native-harness";
import {
  ExperimentalSyncHttpAdapter,
  ExperimentalSyncHttpError,
  createExperimentalCanonicalCborSyncHttpCodec,
  connect,
  type ExperimentalSyncClient,
  type SurrealClient,
} from "react-native-surrealdb";

import { resolveLocalAuthorityUrl } from "../local-authority-url";

type Fixture = {
  marker: "haus-local-sync-fixture-v1";
  phase: "active" | "revoked";
  runSuffix: string;
  householdId: string;
  partitionId: string;
  requestedScope: "recipes-v1";
  subscriptionRevision: 1;
  ownerAccountId: string;
  memberAccountId: string;
  ownerToken: string;
  memberToken: string;
  recipeId: string;
};

declare const require: (path: "./haus-sync-e2e.generated") => Fixture;
const fixture = require("./haus-sync-e2e.generated");
const recipeRecordId = `recipe:${fixture.recipeId}`;
const subscriptionRevision = BigInt(fixture.subscriptionRevision);

type Recipe = {
  id: string;
  recipeName: string;
  languageCode: string;
  source: {
    id: string;
    sourceType: "webpage";
    providerFileId: null;
    originalUrl: string;
    resolvedTitle: string;
    languageHint: string;
    outputLanguage: string;
  };
  ingredients: Array<{
    name: string;
    quantity: string;
    unit: string;
    normalizedQuantity: number;
    normalizedUnit: string;
  }>;
  instructions: Array<{ step: number; instruction: string }>;
  servings: number;
  dietTags: string[];
  prepMinutes: number;
  createdAtIso: string;
  householdId: string;
  createdBy: string;
  updatedBy: string;
};

function recipe(recipeName: string, actor: string): Recipe {
  return {
    id: fixture.recipeId,
    recipeName,
    languageCode: "en",
    source: {
      id: "fixture-source-broccoli-pasta",
      sourceType: "webpage",
      providerFileId: null,
      originalUrl: "https://fixtures.invalid/broccoli-tomato-pasta",
      resolvedTitle: "Canonical comparison fixture",
      languageHint: "en",
      outputLanguage: "English",
    },
    ingredients: [
      {
        name: "broccoli",
        quantity: "300",
        unit: "g",
        normalizedQuantity: 300,
        normalizedUnit: "g",
      },
      {
        name: "tomato",
        quantity: "400",
        unit: "g",
        normalizedQuantity: 400,
        normalizedUnit: "g",
      },
      {
        name: "pasta",
        quantity: "500",
        unit: "g",
        normalizedQuantity: 500,
        normalizedUnit: "g",
      },
      {
        name: "olive oil",
        quantity: "30",
        unit: "ml",
        normalizedQuantity: 30,
        normalizedUnit: "ml",
      },
    ],
    instructions: [
      { step: 1, instruction: "Boil the pasta until al dente." },
    ],
    servings: 4,
    dietTags: ["vegetarian"],
    prepMinutes: 25,
    createdAtIso: "2026-07-28T10:00:00.000Z",
    householdId: fixture.householdId,
    createdBy: actor,
    updatedBy: actor,
  };
}

function makeTransport(
  sync: ExperimentalSyncClient,
  clientId: string,
  accessToken: string,
) {
  return new ExperimentalSyncHttpAdapter({
    sync,
    baseUrl: authorityBaseUrl(),
    allowInsecureLocalhost: true,
    partitionId: fixture.partitionId,
    clientId,
    requestedScope: fixture.requestedScope,
    subscriptionRevision,
    accessToken: () => accessToken,
    codec: createExperimentalCanonicalCborSyncHttpCodec(),
  });
}

function authorityBaseUrl(): string {
  // Force the host loopback so simulator requests cannot hit a LAN listener.
  return resolveLocalAuthorityUrl("http://127.0.0.1:18092");
}

async function close(database: SurrealClient | undefined) {
  if (database && !database.isClosed) await database.close();
}

async function initialPull(
  label: string,
  transport: ExperimentalSyncHttpAdapter,
): Promise<Awaited<ReturnType<ExperimentalSyncHttpAdapter["pull"]>>> {
  try {
    const status = await transport.pull();
    console.info(
      `[Haus Sync E2E] pull ${label} cursorSequence=${status.cursorSequence?.toString() ?? "missing"}`,
    );
    return status;
  } catch (error) {
    const status = error instanceof ExperimentalSyncHttpError
      ? `HTTP ${error.status ?? "unknown"}`
      : "non-HTTP error";
    throw new Error(`${label} failed at /v1/sync/pull: ${status}`);
  }
}

async function tracedPull(
  label: string,
  transport: ExperimentalSyncHttpAdapter,
) {
  const status = await transport.pull();
  console.info(
    `[Haus Sync E2E] pull ${label} cursorSequence=${status.cursorSequence?.toString() ?? "missing"}`,
  );
  return status;
}

function expectAuthorizationFailure(error: unknown) {
  expect(error).toBeInstanceOf(ExperimentalSyncHttpError);
  expect(error).toMatchObject({ kind: "http" });
  expect((error as ExperimentalSyncHttpError).status).toBeGreaterThanOrEqual(400);
  expect((error as ExperimentalSyncHttpError).status).toBeLessThan(500);
}

async function readRecipeProjection(
  database: SurrealClient,
): Promise<{ recipeName: string; householdId: string } | undefined> {
  const [result] = await database.query<
    Array<{ recipeName: string; householdId: string }>
  >(
    "SELECT VALUE { recipeName: recipeName, householdId: householdId } FROM recipe",
  );
  return result?.value.find((recipe) => recipe.recipeName);
}

if (fixture.phase === "active") {
  describe("Hauswirtschaft local authority, active fixture", () => {
    test("creates and pulls a recipe, then reports a simultaneous base-version conflict", async () => {
      const ownerId = `haus-owner-${fixture.runSuffix}`;
      const memberId = `haus-member-${fixture.runSuffix}`;
      let ownerDb: SurrealClient | undefined;
      let memberDb: SurrealClient | undefined;
      let ownerSync: ExperimentalSyncClient | undefined;
      let memberSync: ExperimentalSyncClient | undefined;

      try {
        [ownerDb, memberDb] = await Promise.all([
          connect({
            endpoint: "memory",
            namespace: `haus-sync-${fixture.runSuffix}-${ownerId}`,
            database: "e2e",
          }),
          connect({
            endpoint: "memory",
            namespace: `haus-sync-${fixture.runSuffix}-${memberId}`,
            database: "e2e",
          }),
        ]);
        [ownerSync, memberSync] = await Promise.all([
          ownerDb.openExperimentalSync({
            partitionId: fixture.partitionId,
            clientId: ownerId,
            requestedScope: fixture.requestedScope,
            subscriptionRevision,
          }),
          memberDb.openExperimentalSync({
            partitionId: fixture.partitionId,
            clientId: memberId,
            requestedScope: fixture.requestedScope,
            subscriptionRevision,
          }),
        ]);
        const ownerTransport = makeTransport(ownerSync, ownerId, fixture.ownerToken);
        const memberTransport = makeTransport(
          memberSync,
          memberId,
          fixture.memberToken,
        );

        const authProbe = await fetch(
          `${authorityBaseUrl()}/v1/sync/pull`,
          {
            method: "POST",
            headers: {
              Accept: "application/vnd.surrealdb-sync+cbor",
              Authorization: `Bearer ${fixture.ownerToken}`,
              "Content-Type": "application/vnd.surrealdb-sync+cbor",
            },
            body: new Uint8Array([0]),
          },
        );
        const authority = new URL(authorityBaseUrl());
        console.info(
          `[Haus Sync E2E] owner raw pull auth probe host=${authority.hostname}:${authority.port} HTTP ${authProbe.status}`,
        );
        expect(authProbe.status).toBe(400);

        const [ownerInitial, memberInitial] = await Promise.all([
          initialPull("owner initial pull", ownerTransport),
          initialPull("member initial pull", memberTransport),
        ]);
        expect(ownerInitial).toMatchObject({ pendingCount: 0, conflictCount: 0 });
        expect(memberInitial).toMatchObject({ pendingCount: 0, conflictCount: 0 });

        await ownerSync.enqueue({
          identity: {
            clientCommitId: `create-${fixture.runSuffix}`,
            fingerprint: "computed-by-native",
          },
          operations: [
            {
              kind: "upsert",
              record_id: recipeRecordId,
              base_version: "absent",
              value: recipe("Broccoli Tomato Pasta", fixture.ownerAccountId),
              reference: null,
            },
          ],
        });
        const created = await ownerTransport.push();
        expect(created[0]).toMatchObject({ pendingCount: 0, conflictCount: 0 });

        const memberReceipt = await tracedPull("member after create", memberTransport);
        expect(memberReceipt.cursorSequence).toBeGreaterThan(
          memberInitial.cursorSequence ?? 0n,
        );
        expect(await readRecipeProjection(memberDb)).toEqual({
          recipeName: "Broccoli Tomato Pasta",
          householdId: fixture.householdId,
        });

        await Promise.all([
          tracedPull("owner before concurrent updates", ownerTransport),
          tracedPull("member before concurrent updates", memberTransport),
        ]);
        const memberConflictValue = recipe(
          "Member version",
          fixture.memberAccountId,
        );
        await Promise.all([
          ownerSync.enqueue({
            identity: {
              clientCommitId: `owner-update-${fixture.runSuffix}`,
              fingerprint: "computed-by-native",
            },
            operations: [
              {
                kind: "upsert",
                record_id: recipeRecordId,
                base_version: { exact: 1 },
                value: recipe("Owner version", fixture.ownerAccountId),
                reference: null,
              },
            ],
          }),
          memberSync.enqueue({
            identity: {
              clientCommitId: `member-update-${fixture.runSuffix}`,
              fingerprint: "computed-by-native",
            },
            operations: [
              {
                kind: "upsert",
                record_id: recipeRecordId,
                base_version: { exact: 1 },
                value: memberConflictValue,
                reference: null,
              },
            ],
          }),
        ]);
        const [ownerOptimistic, memberOptimistic] = await Promise.all([
          readRecipeProjection(ownerDb),
          readRecipeProjection(memberDb),
        ]);
        expect(ownerOptimistic?.recipeName).toBe("Owner version");
        expect(memberOptimistic?.recipeName).toBe("Member version");

        const [ownerPush, memberPush] = await Promise.all([
          ownerTransport.push(),
          memberTransport.push(),
        ]);
        const statuses = [ownerPush[0], memberPush[0]];
        expect(statuses).toHaveLength(2);
        expect(statuses.filter((status) => status?.conflictCount === 0)).toHaveLength(1);
        expect(statuses.filter((status) => status?.conflictCount === 1)).toHaveLength(1);
        const loserIndex = statuses.findIndex((status) => status?.conflictCount === 1);
        const loserSync = loserIndex === 0 ? ownerSync : memberSync;
        const loserCommitId = loserIndex === 0
          ? `owner-update-${fixture.runSuffix}`
          : `member-update-${fixture.runSuffix}`;
        expect(await loserSync.conflicts()).toHaveLength(1);
        const loserConflicts = await loserSync.conflicts();
        expect(loserConflicts).toHaveLength(1);
        const serializedConflict = JSON.stringify(
          loserConflicts[0],
          (_key, value) => typeof value === "bigint" ? value.toString() : value,
        );
        expect(serializedConflict).toContain(
          loserIndex === 0 ? "Owner version" : "Member version",
        );

        // The losing value remains durable in the conflict until the app
        // resolves it. Accept the authority value, then use
        // bounded pulls until both materialized app records converge.
        await loserSync.resolveConflictKeepServer(loserCommitId);
        expect(await loserSync.conflicts()).toHaveLength(0);
        let ownerRecipe: Awaited<ReturnType<typeof readRecipeProjection>> | undefined;
        let memberRecipe: Awaited<ReturnType<typeof readRecipeProjection>> | undefined;
        for (let attempt = 0; attempt < 4; attempt += 1) {
          const [ownerPull, memberPull] = await Promise.all([
            tracedPull(`owner after keep-server ${attempt + 1}`, ownerTransport),
            tracedPull(`member after keep-server ${attempt + 1}`, memberTransport),
          ]);
          [ownerRecipe, memberRecipe] = await Promise.all([
            readRecipeProjection(ownerDb),
            readRecipeProjection(memberDb),
          ]);
          console.info(
            `[Haus Sync E2E] resolution projection ${attempt + 1}: owner=${ownerRecipe?.recipeName ?? "missing"}, member=${memberRecipe?.recipeName ?? "missing"}; cursorSequence=${ownerPull.cursorSequence?.toString() ?? "missing"}/${memberPull.cursorSequence?.toString() ?? "missing"}`,
          );
          if (ownerRecipe && JSON.stringify(ownerRecipe) === JSON.stringify(memberRecipe)) {
            break;
          }
        }
        const winnerName = loserIndex === 0 ? "Member version" : "Owner version";
        expect(ownerRecipe?.recipeName).toBe(winnerName);
        expect(memberRecipe).toEqual(ownerRecipe);
        expect(
          (await ownerSync.conflicts()).length +
            (await memberSync.conflicts()).length,
        ).toBe(0);
      } finally {
        await Promise.allSettled([ownerSync?.close(), memberSync?.close()]);
        await Promise.all([close(ownerDb), close(memberDb)]);
      }
    });
  });
} else {
  describe("Hauswirtschaft local authority, revoked fixture", () => {
    test("rejects member push and pull after household membership revocation", async () => {
      const memberId = `haus-member-${fixture.runSuffix}`;
      const replayNamespace = `haus-sync-${fixture.runSuffix}-${memberId}-replay`;
      let database: SurrealClient | undefined;
      let sync: ExperimentalSyncClient | undefined;

      try {
        database = await connect({
          endpoint: "memory",
          namespace: replayNamespace,
          database: "e2e",
        });
        sync = await database.openExperimentalSync({
          partitionId: fixture.partitionId,
          clientId: memberId,
          requestedScope: fixture.requestedScope,
          subscriptionRevision,
        });
        const transport = makeTransport(sync, memberId, fixture.memberToken);
        await sync.enqueue({
          identity: {
            clientCommitId: `member-update-${fixture.runSuffix}`,
            fingerprint: "computed-by-native",
          },
          operations: [
            {
              kind: "upsert",
              record_id: recipeRecordId,
              base_version: { exact: 1 },
              value: recipe("Member version", fixture.memberAccountId),
              reference: null,
            },
          ],
        });

        const pushError = await transport
          .push()
          .then(() => undefined, (error: unknown) => error);
        expectAuthorizationFailure(pushError);
        const pullError = await transport
          .pull()
          .then(() => undefined, (error: unknown) => error);
        expectAuthorizationFailure(pullError);
        expect((await sync.status()).pendingCount).toBe(1);
      } finally {
        await Promise.allSettled([sync?.close()]);
        await close(database);
      }
    });
  });
}
