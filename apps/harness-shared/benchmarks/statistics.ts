export type DistributionSummary = {
  minMs: number;
  medianMs: number;
  p95Ms: number;
  maxMs: number;
  madMs: number;
  operationsPerSecond: number;
};

function percentile(sorted: readonly number[], fraction: number): number {
  if (sorted.length === 0) throw new Error('Cannot summarize zero samples');
  const position = (sorted.length - 1) * fraction;
  const lower = Math.floor(position);
  const upper = Math.ceil(position);
  const weight = position - lower;
  return sorted[lower]! * (1 - weight) + sorted[upper]! * weight;
}

export function summarize(
  samplesMs: readonly number[],
  operationsPerSample: number,
): DistributionSummary {
  if (operationsPerSample <= 0) {
    throw new Error('operationsPerSample must be greater than zero');
  }

  const sorted = [...samplesMs].sort((left, right) => left - right);
  const medianMs = percentile(sorted, 0.5);
  const deviations = sorted
    .map(sample => Math.abs(sample - medianMs))
    .sort((left, right) => left - right);

  return {
    minMs: sorted[0]!,
    medianMs,
    p95Ms: percentile(sorted, 0.95),
    maxMs: sorted.at(-1)!,
    madMs: percentile(deviations, 0.5),
    operationsPerSecond: (operationsPerSample * 1000) / medianMs,
  };
}
