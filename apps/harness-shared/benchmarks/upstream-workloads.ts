/**
 * SurrealQL translation of surrealdb/crud-bench's default workload matrix.
 * Each group links to its pinned upstream definition. See the root
 * THIRD_PARTY_NOTICES.md and PERFORMANCE.md for provenance and license details.
 */
export type ScanProjection = 'count' | 'id' | 'full';

export type ScanIndex = {
  fields: readonly string[];
  type?: 'fulltext';
};

export type ScanSpec = {
  id: string;
  label: string;
  projections: readonly ScanProjection[];
  condition?: string;
  orderBy?: string;
  start?: 'upstream-offset';
  limit?: number;
  expect?: number;
  index?: ScanIndex;
  mixedWrites?: boolean;
};

export type ResolvedScan = Omit<ScanSpec, 'start' | 'limit' | 'expect'> & {
  projection: ScanProjection;
  start?: number;
  limit?: number;
  expect?: number;
};

/**
 * Mobile representation of config/bench.toml from the pinned crud-bench
 * revision. Dialect-specific fields are intentionally reduced to SurrealQL.
 */
export const UPSTREAM_SCAN_SPECS: readonly ScanSpec[] = [
  // Upstream: https://github.com/surrealdb/crud-bench/blob/18eb1fc8d8edcfd3d6ba8328149789ffa7866659/config/bench.toml#L54-L62
  {
    id: 'count',
    label: 'count()',
    projections: ['count'],
  },
  // Upstream: https://github.com/surrealdb/crud-bench/blob/18eb1fc8d8edcfd3d6ba8328149789ffa7866659/config/bench.toml#L64-L80
  {
    id: 'limit',
    label: 'limit(100)',
    projections: ['id', 'full'],
    limit: 100,
    expect: 100,
  },
  // Upstream: https://github.com/surrealdb/crud-bench/blob/18eb1fc8d8edcfd3d6ba8328149789ffa7866659/config/bench.toml#L82-L99
  {
    id: 'start_limit',
    label: 'start(5000) limit(100)',
    projections: ['id', 'full'],
    start: 'upstream-offset',
    limit: 100,
    expect: 100,
  },
  // Upstream: https://github.com/surrealdb/crud-bench/blob/18eb1fc8d8edcfd3d6ba8328149789ffa7866659/config/bench.toml#L101-L136
  {
    id: 'where_field_integer_eq',
    label: 'where(number = 21)',
    projections: ['count', 'full'],
    condition: 'number = 21',
    index: { fields: ['number'] },
    mixedWrites: true,
  },
  // Upstream: https://github.com/surrealdb/crud-bench/blob/18eb1fc8d8edcfd3d6ba8328149789ffa7866659/config/bench.toml#L138-L176
  {
    id: 'where_field_integer_eq_or_eq',
    label: 'where(number = 21 OR number = 22)',
    projections: ['count', 'full'],
    condition: 'number = 21 OR number = 22',
    index: { fields: ['number'] },
    mixedWrites: true,
  },
  // Upstream: https://github.com/surrealdb/crud-bench/blob/18eb1fc8d8edcfd3d6ba8328149789ffa7866659/config/bench.toml#L178-L213
  {
    id: 'where_field_integer_gte_lte',
    label: 'where(number >= 18 AND number <= 21)',
    projections: ['count', 'full'],
    condition: 'number >= 18 AND number <= 21',
    index: { fields: ['number'] },
    mixedWrites: true,
  },
  // Upstream: https://github.com/surrealdb/crud-bench/blob/18eb1fc8d8edcfd3d6ba8328149789ffa7866659/config/bench.toml#L215-L258
  {
    id: 'where_field_integer_in_many',
    label: 'where(number IN [18, 21, 30, 40, 50, 60]) limit(1000)',
    projections: ['count', 'full'],
    condition: 'number IN [18, 21, 30, 40, 50, 60]',
    limit: 1_000,
    index: { fields: ['number'] },
    mixedWrites: true,
  },
  // Upstream: https://github.com/surrealdb/crud-bench/blob/18eb1fc8d8edcfd3d6ba8328149789ffa7866659/config/bench.toml#L260-L304
  {
    id: 'where_field_string_eq_order_by_desc',
    label: "where(status = 'published') order(created_at DESC) limit(1000)",
    projections: ['count', 'full'],
    condition: "status = 'published'",
    orderBy: 'created_at DESC',
    limit: 1_000,
    index: { fields: ['status', 'created_at'] },
    mixedWrites: true,
  },
  // Upstream: https://github.com/surrealdb/crud-bench/blob/18eb1fc8d8edcfd3d6ba8328149789ffa7866659/config/bench.toml#L306-L350
  {
    id: 'where_field_integer_in_many_order_by_desc',
    label:
      'where(http_status IN [200, 201]) order(created_at DESC) limit(1000)',
    projections: ['count', 'full'],
    condition: 'http_status IN [200, 201]',
    orderBy: 'created_at DESC',
    limit: 1_000,
    index: { fields: ['http_status', 'created_at'] },
    mixedWrites: true,
  },
  // Upstream: https://github.com/surrealdb/crud-bench/blob/18eb1fc8d8edcfd3d6ba8328149789ffa7866659/config/bench.toml#L352-L398
  {
    id: 'where_field_many_contains_string_order_by_desc',
    label: "where(tags CONTAINS 'omicron') order(created_at DESC) limit(1000)",
    projections: ['count', 'full'],
    condition: "tags CONTAINS 'omicron'",
    orderBy: 'created_at DESC',
    limit: 1_000,
    index: { fields: ['tags.*', 'created_at'] },
    mixedWrites: true,
  },
  // Upstream: https://github.com/surrealdb/crud-bench/blob/18eb1fc8d8edcfd3d6ba8328149789ffa7866659/config/bench.toml#L400-L420
  {
    id: 'where_field_fulltext_single',
    label: "where(words @@ 'hello') limit(1000) - BM25",
    projections: ['full'],
    condition: "words @@ 'hello'",
    limit: 1_000,
    index: { fields: ['words'], type: 'fulltext' },
  },
  // Upstream: https://github.com/surrealdb/crud-bench/blob/18eb1fc8d8edcfd3d6ba8328149789ffa7866659/config/bench.toml#L422-L442
  {
    id: 'where_field_fulltext_multi_and',
    label: "where(words @@ 'hello' AND words @@ 'world') limit(1000) - BM25",
    projections: ['full'],
    condition: "words @@ 'hello' AND words @@ 'world'",
    limit: 1_000,
    index: { fields: ['words'], type: 'fulltext' },
  },
  // Upstream: https://github.com/surrealdb/crud-bench/blob/18eb1fc8d8edcfd3d6ba8328149789ffa7866659/config/bench.toml#L444-L464
  {
    id: 'where_field_fulltext_multi_or',
    label: "where(words @@ 'foo' OR words @@ 'bar') limit(1000) - BM25",
    projections: ['full'],
    condition: "words @@ 'foo' OR words @@ 'bar'",
    limit: 1_000,
    index: { fields: ['words'], type: 'fulltext' },
  },
];

export function resolveScans(records: number): ResolvedScan[] {
  return UPSTREAM_SCAN_SPECS.flatMap(spec =>
    spec.projections.map(projection => ({
      ...spec,
      projection,
      start:
        spec.start === 'upstream-offset'
          ? Math.min(5_000, Math.max(0, records - 100))
          : undefined,
      limit:
        spec.limit === undefined ? undefined : Math.min(spec.limit, records),
      expect:
        spec.expect === undefined ? undefined : Math.min(spec.expect, records),
    })),
  );
}

export function buildScanQuery(scan: ResolvedScan): string {
  const filter = scan.condition ? `WHERE ${scan.condition}` : '';
  const order = scan.orderBy ? `ORDER BY ${scan.orderBy}` : '';
  const start = scan.start === undefined ? '' : `START ${scan.start}`;
  const limit = scan.limit === undefined ? '' : `LIMIT ${scan.limit}`;

  if (scan.projection === 'count') {
    if (!start && !limit) {
      return compact(
        `SELECT count() FROM benchmark_record ${filter} GROUP ALL`,
      );
    }
    // Upstream COUNT run definitions omit ORDER BY even when the paired FULL
    // run has it: https://github.com/surrealdb/crud-bench/blob/18eb1fc8d8edcfd3d6ba8328149789ffa7866659/config/bench.toml#L264-L291
    return compact(
      `SELECT count() FROM (SELECT 1 FROM benchmark_record ${filter} ${start} ${limit}) GROUP ALL`,
    );
  }

  const projection = scan.projection === 'id' ? 'id' : '*';
  return compact(
    `SELECT ${projection} FROM benchmark_record ${filter} ${order} ${start} ${limit}`,
  );
}

export function scanMetricStem(scan: ResolvedScan): string {
  return `scan.${scan.id}.${scan.projection}`;
}

export function scanIndexName(scan: ResolvedScan): string {
  return `rn_${scan.id}_${scan.projection}`.replace(/[^a-z0-9_]/g, '_');
}

export function indexBuildQuery(scan: ResolvedScan): string {
  if (!scan.index) throw new Error(`Scan ${scan.id} does not define an index`);
  const name = scanIndexName(scan);
  const fields = scan.index.fields.join(', ');
  if (scan.index.type === 'fulltext') {
    return compact(`
      DEFINE ANALYZER IF NOT EXISTS ${name}
        TOKENIZERS blank,class FILTERS lowercase,ascii;
      DEFINE INDEX ${name} ON TABLE benchmark_record FIELDS ${fields}
        FULLTEXT ANALYZER ${name} BM25;
    `);
  }
  return `DEFINE INDEX ${name} ON TABLE benchmark_record FIELDS ${fields}`;
}

export function indexDropQuery(scan: ResolvedScan): string {
  const name = scanIndexName(scan);
  const removeIndex = `REMOVE INDEX IF EXISTS ${name} ON TABLE benchmark_record`;
  return scan.index?.type === 'fulltext'
    ? `${removeIndex}; REMOVE ANALYZER IF EXISTS ${name}`
    : removeIndex;
}

export function mutableIndexField(scan: ResolvedScan): string {
  const field = scan.index?.fields[0];
  if (!field) throw new Error(`Scan ${scan.id} has no mutable index field`);
  return field.replace(/\.\*$/, '');
}

function compact(query: string): string {
  return query.replace(/\s+/g, ' ').trim();
}
