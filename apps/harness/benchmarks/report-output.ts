import { Platform } from 'react-native';

export const BENCHMARK_RESULT_CHUNK_MARKER =
  'SURREALDB_BENCHMARK_RESULT_CHUNK=';

const MAX_CHUNK_LENGTH = 2_800;
const REPORT_RECEIVER_PORT = 18_082;

/**
 * Emits a report in lines small enough for Android's native log buffer.
 * The host runner reconstructs the JSON from the numbered chunks.
 */
export async function emitBenchmarkReport(report: {
  measuredAt: string;
  metrics: readonly unknown[];
}) {
  const encoded = JSON.stringify(report);
  const runId = Date.parse(report.measuredAt);
  if (!Number.isSafeInteger(runId)) {
    throw new Error(`Invalid benchmark timestamp: ${report.measuredAt}`);
  }
  const chunks: string[] = [];
  for (let offset = 0; offset < encoded.length; offset += MAX_CHUNK_LENGTH) {
    chunks.push(encoded.slice(offset, offset + MAX_CHUNK_LENGTH));
  }

  chunks.forEach((chunk, index) => {
    console.warn(
      `${BENCHMARK_RESULT_CHUNK_MARKER}${runId}:${index + 1}/${
        chunks.length
      }:${chunk}`,
    );
  });

  const host = Platform.OS === 'android' ? '10.0.2.2' : '127.0.0.1';
  const response = await fetch(
    `http://${host}:${REPORT_RECEIVER_PORT}/benchmark-report`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: encoded,
    },
  );
  if (!response.ok) {
    throw new Error(
      `Benchmark report receiver returned HTTP ${response.status}`,
    );
  }
}
