import type { MobileBenchmarkReport } from './surreal-crud';

export const BENCHMARK_RESULT_CHUNK_MARKER =
  'SURREALDB_BENCHMARK_RESULT_CHUNK=';

const MAX_CHUNK_LENGTH = 2_800;

/**
 * Emits a report in lines small enough for Android's native log buffer.
 * The host runner reconstructs the JSON from the numbered chunks.
 */
export function emitBenchmarkReport(report: MobileBenchmarkReport) {
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
}
