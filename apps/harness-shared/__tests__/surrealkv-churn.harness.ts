import { describe, expect, test } from "react-native-harness";
import { connect } from "react-native-surrealdb";

import {
  surrealkvChurnEndpoint,
  surrealkvChurnIterations,
} from "../surrealkv-churn-fixture";

const connectionOptions = {
  endpoint: surrealkvChurnEndpoint,
  namespace: "surrealkv-churn-e2e",
  database: "surrealkv-churn-e2e",
} as const;

describe("SurrealKV lifecycle churn", () => {
  test("repeatedly opens, mutates, reads, and closes durable storage", async () => {
    const setup = await connect(connectionOptions);
    await setup.query("REMOVE TABLE IF EXISTS churn_probe");
    await setup.close();

    for (
      let iteration = 1;
      iteration <= surrealkvChurnIterations;
      iteration++
    ) {
      const database = await connect(connectionOptions);
      await database.query(
        "UPSERT churn_probe:marker SET iteration = $iteration RETURN NONE",
        { iteration }
      );
      const [result] = await database.query<bigint[]>(
        "SELECT VALUE iteration FROM churn_probe:marker"
      );
      expect(result?.value).toEqual([BigInt(iteration)]);
      await database.close();
    }

    const verification = await connect(connectionOptions);
    const [result] = await verification.query<bigint[]>(
      "SELECT VALUE iteration FROM churn_probe:marker"
    );
    expect(result?.value).toEqual([BigInt(surrealkvChurnIterations)]);
    await verification.query("REMOVE TABLE IF EXISTS churn_probe");
    await verification.close();
  });
});
