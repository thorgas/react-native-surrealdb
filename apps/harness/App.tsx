import {
  Profiler,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import {
  ActivityIndicator,
  Linking,
  Platform,
  Pressable,
  ScrollView,
  Share,
  StatusBar,
  StyleSheet,
  Text,
  useColorScheme,
  View,
} from 'react-native';
import { SafeAreaProvider, SafeAreaView } from 'react-native-safe-area-context';

import {
  CANONICAL_BENCHMARK_OPTIONS,
  CRUD_BENCH_SOURCE,
  runSurrealCrudBenchmark,
  SMOKE_BENCHMARK_OPTIONS,
  UPSTREAM_BENCHMARK_OPTIONS,
  type MobileBenchmarkProfile,
  type MobileBenchmarkProgress,
  type MobileBenchmarkReport,
} from './benchmarks/surreal-crud';
import {
  runSQLiteBenchBenchmark,
  SQLITE_BENCH_COOLDOWN_MS,
  SQLITE_BENCH_ITERATIONS,
  SQLITE_BENCH_PUBLISHED_RESULTS,
  SQLITE_BENCH_SOURCE,
  type SQLiteBenchProgress,
  type SQLiteBenchReport,
} from './benchmarks/sqlite-bench';
import {
  runStartupLoadBenchmark,
  STARTUP_LOAD_RECORD_COUNT,
  type StartupLoadProgress,
  type StartupLoadReport,
  type StartupRenderEntry,
} from './benchmarks/startup-load';

const PROFILES = {
  smoke: {
    label: 'Smoke',
    description: '200 records · quickest full-matrix check',
    options: SMOKE_BENCHMARK_OPTIONS,
  },
  canonical: {
    label: 'Canonical',
    description: '2,000 records · stable regression profile',
    options: CANONICAL_BENCHMARK_OPTIONS,
  },
  upstream: {
    label: 'Full matrix',
    description: '10,000 records · preserves upstream START 5000',
    options: UPSTREAM_BENCHMARK_OPTIONS,
  },
} as const;

type StartupRenderTiming = {
  reactRenderMs: number;
  renderToLayoutMs: number;
};

function App() {
  const isDark = useColorScheme() === 'dark';
  return (
    <SafeAreaProvider>
      <StatusBar barStyle={isDark ? 'light-content' : 'dark-content'} />
      <BenchmarkScreen isDark={isDark} />
    </SafeAreaProvider>
  );
}

function BenchmarkScreen({ isDark }: { isDark: boolean }) {
  const [profile, setProfile] = useState<MobileBenchmarkProfile>('smoke');
  const [progress, setProgress] = useState<MobileBenchmarkProgress>();
  const [report, setReport] = useState<MobileBenchmarkReport>();
  const [error, setError] = useState<string>();
  const [running, setRunning] = useState(false);
  const controller = useRef<AbortController | undefined>(undefined);
  const startupController = useRef<AbortController | undefined>(undefined);
  const sqliteBenchController = useRef<AbortController | undefined>(undefined);
  const [startupProgress, setStartupProgress] = useState<StartupLoadProgress>();
  const [startupReport, setStartupReport] = useState<StartupLoadReport>();
  const [startupEntries, setStartupEntries] = useState<StartupRenderEntry[]>(
    [],
  );
  const [startupRenderTiming, setStartupRenderTiming] =
    useState<StartupRenderTiming>();
  const [startupError, setStartupError] = useState<string>();
  const [startupRunning, setStartupRunning] = useState(false);
  const [sqliteBenchProgress, setSQLiteBenchProgress] =
    useState<SQLiteBenchProgress>();
  const [sqliteBenchReport, setSQLiteBenchReport] =
    useState<SQLiteBenchReport>();
  const [sqliteBenchError, setSQLiteBenchError] = useState<string>();
  const [sqliteBenchRunning, setSQLiteBenchRunning] = useState(false);
  const renderRequestedAt = useRef<number | undefined>(undefined);
  const reactRenderMs = useRef<number | undefined>(undefined);
  const renderToLayoutMs = useRef<number | undefined>(undefined);
  const colors = isDark ? darkColors : lightColors;

  const finishRenderTiming = useCallback(
    (startupLoadReport: StartupLoadReport | undefined) => {
      if (
        !startupLoadReport ||
        reactRenderMs.current === undefined ||
        renderToLayoutMs.current === undefined
      ) {
        return;
      }
      const timing = {
        reactRenderMs: reactRenderMs.current,
        renderToLayoutMs: renderToLayoutMs.current,
      };
      setStartupRenderTiming(timing);
      console.warn(
        `SURREALDB_STARTUP_TIMING=${JSON.stringify({
          platform: Platform.OS,
          records: startupLoadReport.rowsLoaded,
          fetchMs: startupLoadReport.timingsMs.queryAndDecode,
          reactRenderMs: timing.reactRenderMs,
          renderToLayoutMs: timing.renderToLayoutMs,
          seedMs: startupLoadReport.timingsMs.seed,
          readyBeforeRenderMs: startupLoadReport.timingsMs.ready,
        })}`,
      );
    },
    [],
  );

  const runStartup = useCallback(async () => {
    const abortController = new AbortController();
    startupController.current?.abort();
    startupController.current = abortController;
    setStartupRunning(true);
    setStartupError(undefined);
    setStartupReport(undefined);
    setStartupEntries([]);
    setStartupRenderTiming(undefined);
    setStartupProgress({ stage: 'open', completed: 0, total: 1 });
    renderRequestedAt.current = undefined;
    reactRenderMs.current = undefined;
    renderToLayoutMs.current = undefined;

    try {
      const result = await runStartupLoadBenchmark({
        signal: abortController.signal,
        onProgress: setStartupProgress,
      });
      if (!abortController.signal.aborted) {
        renderRequestedAt.current = performance.now();
        setStartupReport(result.report);
        setStartupEntries(result.entries);
      }
    } catch (cause) {
      if (!abortController.signal.aborted) {
        setStartupError(cause instanceof Error ? cause.message : String(cause));
      }
    } finally {
      if (
        startupController.current === abortController &&
        !abortController.signal.aborted
      ) {
        startupController.current = undefined;
        setStartupRunning(false);
      }
    }
  }, []);

  const onStartupRender = useCallback(
    (
      _id: string,
      phase: 'mount' | 'update' | 'nested-update',
      actualDuration: number,
    ) => {
      if (
        phase === 'mount' &&
        startupEntries.length === STARTUP_LOAD_RECORD_COUNT &&
        reactRenderMs.current === undefined
      ) {
        reactRenderMs.current = actualDuration;
        finishRenderTiming(startupReport);
      }
    },
    [finishRenderTiming, startupEntries.length, startupReport],
  );

  const onStartupLayout = useCallback(() => {
    if (
      startupEntries.length !== STARTUP_LOAD_RECORD_COUNT ||
      renderRequestedAt.current === undefined ||
      renderToLayoutMs.current !== undefined
    ) {
      return;
    }
    renderToLayoutMs.current = performance.now() - renderRequestedAt.current;
    finishRenderTiming(startupReport);
  }, [finishRenderTiming, startupEntries.length, startupReport]);

  const runSQLiteBench = useCallback(async () => {
    const abortController = new AbortController();
    sqliteBenchController.current?.abort();
    sqliteBenchController.current = abortController;
    setSQLiteBenchRunning(true);
    setSQLiteBenchError(undefined);
    setSQLiteBenchReport(undefined);
    setStartupEntries([]);
    setSQLiteBenchProgress({
      completed: 0,
      total: 3,
      metric: 'database setup',
      stage: 'setup',
    });

    try {
      const result = await runSQLiteBenchBenchmark({
        platform: Platform.OS === 'ios' ? 'ios' : 'android',
        device:
          Platform.OS === 'android'
            ? Platform.constants.Model
            : 'iPhone simulator',
        os: String(Platform.Version),
        reactNative: '0.86.0',
        surrealDb: '3.2.1',
        signal: abortController.signal,
        onProgress: setSQLiteBenchProgress,
      });
      if (!abortController.signal.aborted) {
        setSQLiteBenchReport(result);
        console.warn(`SURREALDB_SQLITE_BENCH_TIMING=${JSON.stringify(result)}`);
      }
    } catch (cause) {
      if (!abortController.signal.aborted) {
        setSQLiteBenchError(
          cause instanceof Error ? cause.message : String(cause),
        );
      }
    } finally {
      if (
        sqliteBenchController.current === abortController &&
        !abortController.signal.aborted
      ) {
        sqliteBenchController.current = undefined;
        setSQLiteBenchRunning(false);
      }
    }
  }, []);

  useEffect(() => {
    runStartup().catch(() => undefined);
    return () => {
      startupController.current?.abort();
      sqliteBenchController.current?.abort();
    };
  }, [runStartup]);

  const percent = useMemo(() => {
    if (!progress?.total) return 0;
    return Math.min(100, (progress.completed / progress.total) * 100);
  }, [progress]);

  const startupPercent = useMemo(() => {
    if (!startupProgress?.total) return 0;
    return Math.min(
      100,
      (startupProgress.completed / startupProgress.total) * 100,
    );
  }, [startupProgress]);

  const start = async () => {
    const abortController = new AbortController();
    controller.current = abortController;
    setRunning(true);
    setError(undefined);
    setReport(undefined);
    setProgress({ completed: 0, total: 0, metric: 'starting', stage: 'setup' });

    try {
      const result = await runSurrealCrudBenchmark({
        ...PROFILES[profile].options,
        platform: Platform.OS === 'ios' ? 'ios' : 'android',
        device:
          Platform.OS === 'android'
            ? Platform.constants.Model
            : 'iPhone simulator',
        os: String(Platform.Version),
        reactNative: '0.86.0',
        surrealDb: '3.2.1',
        signal: abortController.signal,
        onProgress: setProgress,
      });
      setReport(result);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      if (controller.current === abortController)
        controller.current = undefined;
      setRunning(false);
    }
  };

  const cancel = () => {
    controller.current?.abort();
    setProgress(current =>
      current ? { ...current, metric: 'cancelling…' } : current,
    );
  };

  const share = async () => {
    if (!report) return;
    await Share.share({
      title: `SurrealDB ${report.configuration.profile} benchmark`,
      message: JSON.stringify(report, null, 2),
    });
  };

  return (
    <SafeAreaView style={[styles.safeArea, { backgroundColor: colors.page }]}>
      <ScrollView
        contentContainerStyle={styles.content}
        keyboardShouldPersistTaps="handled"
      >
        <View style={styles.header}>
          <Text style={[styles.eyebrow, { color: colors.accent }]}>
            SURRΞALDB
          </Text>
          <Text style={[styles.title, { color: colors.text }]}>
            Mobile benchmark lab
          </Text>
          <Text style={[styles.subtitle, { color: colors.muted }]}>
            Run the pinned crud-bench default workload matrix through Hermes,
            JSI, UniFFI, Rust, and embedded SurrealDB on this device.
          </Text>
        </View>

        <StartupLoadCard
          colors={colors}
          error={startupError}
          onRun={runStartup}
          percent={startupPercent}
          progress={startupProgress}
          report={startupReport}
          renderTiming={startupRenderTiming}
          running={startupRunning}
        />

        {startupEntries.length ? (
          <Profiler id="startup-10k-render" onRender={onStartupRender}>
            <View
              accessibilityElementsHidden
              importantForAccessibility="no-hide-descendants"
              onLayout={onStartupLayout}
              pointerEvents="none"
              style={styles.renderProbe}
            >
              {startupEntries.map(entry => (
                <Text key={entry.sequence} style={styles.renderProbeText}>
                  {entry.sequence} · {entry.label} · {entry.bucket} ·{' '}
                  {entry.active ? 'active' : 'inactive'} · {entry.score}
                </Text>
              ))}
            </View>
          </Profiler>
        ) : null}

        <SQLiteBenchCard
          colors={colors}
          error={sqliteBenchError}
          onRun={runSQLiteBench}
          progress={sqliteBenchProgress}
          report={sqliteBenchReport}
          running={sqliteBenchRunning}
        />

        <View style={styles.profileGrid}>
          {(Object.keys(PROFILES) as MobileBenchmarkProfile[]).map(key => {
            const selected = profile === key;
            return (
              <Pressable
                accessibilityRole="radio"
                accessibilityState={{ checked: selected, disabled: running }}
                disabled={running}
                key={key}
                onPress={() => setProfile(key)}
                style={({ pressed }) => [
                  styles.profile,
                  {
                    backgroundColor: selected ? colors.selected : colors.card,
                    borderColor: selected ? colors.accent : colors.border,
                    opacity: pressed ? 0.75 : 1,
                  },
                ]}
              >
                <Text style={[styles.profileTitle, { color: colors.text }]}>
                  {PROFILES[key].label}
                </Text>
                <Text
                  style={[styles.profileDescription, { color: colors.muted }]}
                >
                  {PROFILES[key].description}
                </Text>
              </Pressable>
            );
          })}
        </View>

        <View
          style={[
            styles.panel,
            { backgroundColor: colors.card, borderColor: colors.border },
          ]}
        >
          <View style={styles.panelRow}>
            <View style={styles.panelCopy}>
              <Text style={[styles.panelTitle, { color: colors.text }]}>
                {running ? 'Benchmark running' : 'Ready on this device'}
              </Text>
              <Text style={[styles.panelDetail, { color: colors.muted }]}>
                {running
                  ? progress?.metric
                  : '141 measured variants · raw samples, median, p95, MAD and ops/s'}
              </Text>
            </View>
            {running ? <ActivityIndicator color={colors.accent} /> : null}
          </View>

          {running ? (
            <>
              <View style={[styles.track, { backgroundColor: colors.track }]}>
                <View
                  style={[
                    styles.fill,
                    { backgroundColor: colors.accent, width: `${percent}%` },
                  ]}
                />
              </View>
              <Text style={[styles.progressText, { color: colors.muted }]}>
                {progress?.total
                  ? `${progress.completed} / ${
                      progress.total
                    } workloads · ${percent.toFixed(0)}%`
                  : 'Preparing deterministic records…'}
              </Text>
            </>
          ) : null}

          <Pressable
            accessibilityRole="button"
            onPress={running ? cancel : start}
            style={({ pressed }) => [
              styles.primaryButton,
              {
                backgroundColor: running ? colors.danger : colors.accent,
                opacity: pressed ? 0.8 : 1,
              },
            ]}
          >
            <Text style={styles.primaryButtonText}>
              {running ? 'Cancel benchmark' : `Run ${PROFILES[profile].label}`}
            </Text>
          </Pressable>
        </View>

        {error ? (
          <View
            style={[
              styles.message,
              {
                backgroundColor: colors.errorSurface,
                borderColor: colors.danger,
              },
            ]}
          >
            <Text style={[styles.messageTitle, { color: colors.danger }]}>
              Run failed
            </Text>
            <Text
              selectable
              style={[styles.messageBody, { color: colors.text }]}
            >
              {error}
            </Text>
          </View>
        ) : null}

        {report ? (
          <Results report={report} colors={colors} onShare={share} />
        ) : null}

        <View style={[styles.note, { borderColor: colors.border }]}>
          <Text style={[styles.noteTitle, { color: colors.text }]}>
            Methodology
          </Text>
          <Text style={[styles.noteBody, { color: colors.muted }]}>
            Pinned to {CRUD_BENCH_SOURCE.revision.slice(0, 12)}. Mobile results
            are regression signals for matching devices and configurations, not
            direct comparisons with SurrealDB&apos;s server benchmark hardware.
            Vector KNN remains a separate upstream suite.
          </Text>
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}

function SQLiteBenchCard({
  colors,
  error,
  onRun,
  progress,
  report,
  running,
}: {
  colors: typeof lightColors;
  error?: string;
  onRun: () => void;
  progress?: SQLiteBenchProgress;
  report?: SQLiteBenchReport;
  running: boolean;
}) {
  const percent = progress ? (progress.completed / progress.total) * 100 : 0;

  return (
    <View
      style={[
        styles.panel,
        { backgroundColor: colors.card, borderColor: colors.border },
      ]}
    >
      <View style={styles.panelRow}>
        <View style={styles.panelCopy}>
          <Text style={[styles.panelTitle, { color: colors.text }]}>
            SQLite benchmark adaptation
          </Text>
          <Text style={[styles.panelDetail, { color: colors.muted }]}>
            In-memory SurrealDB · {SQLITE_BENCH_ITERATIONS.toLocaleString()}{' '}
            async inserts, transaction inserts, and full-table selects ·{' '}
            {SQLITE_BENCH_COOLDOWN_MS.toLocaleString()} ms cooldown
          </Text>
          <Text
            accessibilityRole="link"
            onPress={() => {
              Linking.openURL(SQLITE_BENCH_SOURCE.url).catch(() => undefined);
            }}
            style={[styles.sourceLink, { color: colors.accent }]}
          >
            Adapted from ospfranco/sqlite-bench @{' '}
            {SQLITE_BENCH_SOURCE.revision.slice(0, 7)}
          </Text>
        </View>
        {running ? <ActivityIndicator color={colors.accent} /> : null}
      </View>

      {progress ? (
        <Text style={[styles.progressText, { color: colors.muted }]}>
          {error ??
            (progress.stage === 'complete'
              ? 'Complete'
              : `${progress.metric} · ${progress.completed} / ${progress.total}`)}
        </Text>
      ) : null}

      {running ? (
        <View style={[styles.track, { backgroundColor: colors.track }]}>
          <View
            style={[
              styles.fill,
              { backgroundColor: colors.accent, width: `${percent}%` },
            ]}
          />
        </View>
      ) : null}

      {report ? (
        <>
          <Text style={[styles.comparisonTitle, { color: colors.text }]}>
            SurrealDB on this device
          </Text>
          <View style={styles.startupMetrics}>
            {report.metrics.map(metric => (
              <MetricNumber
                color={colors.text}
                key={metric.name}
                label={metric.upstreamCase}
                muted={colors.muted}
                value={`${metric.summary.medianMs.toFixed(1)} ms`}
              />
            ))}
          </View>

          <Text style={[styles.comparisonTitle, { color: colors.text }]}>
            Comparable published SQLite rows
          </Text>
          <Text style={[styles.referenceNote, { color: colors.muted }]}>
            Different, unspecified environment — context only, not a regression
            baseline.
          </Text>
          {SQLITE_BENCH_PUBLISHED_RESULTS.libraries.map(library => (
            <Text
              key={library.name}
              style={[styles.referenceRow, { color: colors.muted }]}
            >
              <Text style={[styles.referenceLibrary, { color: colors.text }]}>
                {library.name}
              </Text>
              {' · '}async {library.asyncInsertMs.toFixed(1)} ms · tx{' '}
              {library.transactionInsertMs.toFixed(1)} ms · select{' '}
              {library.selectAndReadMs.toFixed(1)} ms
            </Text>
          ))}
        </>
      ) : null}

      {!running ? (
        <Pressable
          accessibilityRole="button"
          onPress={onRun}
          style={({ pressed }) => [
            styles.secondaryButton,
            {
              borderColor: colors.accent,
              backgroundColor: pressed ? colors.selected : 'transparent',
            },
          ]}
        >
          <Text style={[styles.shareText, { color: colors.accent }]}>
            {report ? 'Run SQLite adaptation again' : 'Run SQLite adaptation'}
          </Text>
        </Pressable>
      ) : null}
    </View>
  );
}

function StartupLoadCard({
  colors,
  error,
  onRun,
  percent,
  progress,
  report,
  renderTiming,
  running,
}: {
  colors: typeof lightColors;
  error?: string;
  onRun: () => void;
  percent: number;
  progress?: StartupLoadProgress;
  report?: StartupLoadReport;
  renderTiming?: StartupRenderTiming;
  running: boolean;
}) {
  const status =
    progress?.stage === 'open'
      ? 'Opening embedded database…'
      : progress?.stage === 'seed'
      ? `Creating ${progress.completed.toLocaleString()} / ${progress.total.toLocaleString()} records…`
      : progress?.stage === 'load'
      ? 'Querying and decoding all records…'
      : report
      ? `${report.rowsLoaded.toLocaleString()} records loaded and verified`
      : 'Waiting to start…';

  return (
    <View
      style={[
        styles.panel,
        { backgroundColor: colors.card, borderColor: colors.border },
      ]}
    >
      <View style={styles.panelRow}>
        <View style={styles.panelCopy}>
          <Text style={[styles.panelTitle, { color: colors.text }]}>
            10k startup load
          </Text>
          <Text style={[styles.panelDetail, { color: colors.muted }]}>
            Automatically creates, queries, decodes, and fully reads{' '}
            {STARTUP_LOAD_RECORD_COUNT.toLocaleString()} entries at app startup.
          </Text>
        </View>
        {running ? <ActivityIndicator color={colors.accent} /> : null}
      </View>

      <Text style={[styles.progressText, { color: colors.muted }]}>
        {error ?? status}
      </Text>

      {running ? (
        <View style={[styles.track, { backgroundColor: colors.track }]}>
          <View
            style={[
              styles.fill,
              { backgroundColor: colors.accent, width: `${percent}%` },
            ]}
          />
        </View>
      ) : null}

      {report ? (
        <View style={styles.startupMetrics}>
          <MetricNumber
            label="open"
            value={`${report.timingsMs.open.toFixed(1)} ms`}
            color={colors.text}
            muted={colors.muted}
          />
          <MetricNumber
            label="ready"
            value={`${report.timingsMs.ready.toFixed(1)} ms`}
            color={colors.text}
            muted={colors.muted}
          />
          <MetricNumber
            label="seed"
            value={`${report.timingsMs.seed.toFixed(1)} ms`}
            color={colors.text}
            muted={colors.muted}
          />
          <MetricNumber
            label="query + decode"
            value={`${report.timingsMs.queryAndDecode.toFixed(1)} ms`}
            color={colors.text}
            muted={colors.muted}
          />
          <MetricNumber
            label="materialize"
            value={`${report.timingsMs.materialize.toFixed(1)} ms`}
            color={colors.text}
            muted={colors.muted}
          />
          <MetricNumber
            label="React render"
            value={
              renderTiming
                ? `${renderTiming.reactRenderMs.toFixed(1)} ms`
                : 'measuring…'
            }
            color={colors.text}
            muted={colors.muted}
          />
          <MetricNumber
            label="render → layout"
            value={
              renderTiming
                ? `${renderTiming.renderToLayoutMs.toFixed(1)} ms`
                : 'measuring…'
            }
            color={colors.text}
            muted={colors.muted}
          />
        </View>
      ) : null}

      {!running ? (
        <Pressable
          accessibilityRole="button"
          onPress={onRun}
          style={({ pressed }) => [
            styles.secondaryButton,
            {
              borderColor: colors.accent,
              backgroundColor: pressed ? colors.selected : 'transparent',
            },
          ]}
        >
          <Text style={[styles.shareText, { color: colors.accent }]}>
            Run startup load again
          </Text>
        </Pressable>
      ) : null}
    </View>
  );
}

function Results({
  report,
  colors,
  onShare,
}: {
  report: MobileBenchmarkReport;
  colors: typeof lightColors;
  onShare: () => void;
}) {
  return (
    <View style={styles.results}>
      <View style={styles.resultsHeader}>
        <View>
          <Text style={[styles.resultsTitle, { color: colors.text }]}>
            Results
          </Text>
          <Text style={[styles.resultsMeta, { color: colors.muted }]}>
            {report.metrics.length} metrics ·{' '}
            {report.configuration.records.toLocaleString()} records
          </Text>
        </View>
        <Pressable
          accessibilityRole="button"
          onPress={onShare}
          style={({ pressed }) => [
            styles.shareButton,
            {
              borderColor: colors.accent,
              backgroundColor: pressed ? colors.selected : 'transparent',
            },
          ]}
        >
          <Text style={[styles.shareText, { color: colors.accent }]}>
            Share JSON
          </Text>
        </Pressable>
      </View>

      {report.metrics.map(metric => (
        <View
          key={metric.name}
          style={[
            styles.metric,
            { backgroundColor: colors.card, borderColor: colors.border },
          ]}
        >
          <Text selectable style={[styles.metricName, { color: colors.text }]}>
            {metric.name}
          </Text>
          <Text style={[styles.metricVariant, { color: colors.muted }]}>
            {metric.variant}
          </Text>
          <View style={styles.metricNumbers}>
            <MetricNumber
              label="median"
              value={`${metric.summary.medianMs.toFixed(3)} ms`}
              color={colors.text}
              muted={colors.muted}
            />
            <MetricNumber
              label="p95"
              value={`${metric.summary.p95Ms.toFixed(3)} ms`}
              color={colors.text}
              muted={colors.muted}
            />
            <MetricNumber
              label="ops/s"
              value={metric.summary.operationsPerSecond.toFixed(1)}
              color={colors.text}
              muted={colors.muted}
            />
          </View>
        </View>
      ))}
    </View>
  );
}

function MetricNumber({
  label,
  value,
  color,
  muted,
}: {
  label: string;
  value: string;
  color: string;
  muted: string;
}) {
  return (
    <View>
      <Text style={[styles.metricLabel, { color: muted }]}>{label}</Text>
      <Text style={[styles.metricValue, { color }]}>{value}</Text>
    </View>
  );
}

const lightColors = {
  page: '#F4F6F2',
  card: '#FFFFFF',
  selected: '#E5F5EE',
  text: '#15251E',
  muted: '#617068',
  accent: '#08A66C',
  border: '#D5DDD8',
  track: '#DDE8E2',
  danger: '#C13B46',
  errorSurface: '#FBEAEC',
};

const darkColors: typeof lightColors = {
  page: '#0D1512',
  card: '#15201B',
  selected: '#173C2E',
  text: '#EEF6F1',
  muted: '#9DAEA5',
  accent: '#35D69A',
  border: '#2A3B33',
  track: '#23332C',
  danger: '#FF737D',
  errorSurface: '#3B2023',
};

const styles = StyleSheet.create({
  safeArea: { flex: 1 },
  content: { padding: 20, paddingBottom: 48, gap: 18 },
  header: { gap: 8, marginTop: 8 },
  eyebrow: { fontSize: 13, fontWeight: '800', letterSpacing: 2.5 },
  title: { fontSize: 34, fontWeight: '800', letterSpacing: -1.1 },
  subtitle: { fontSize: 16, lineHeight: 23, maxWidth: 620 },
  profileGrid: { gap: 10 },
  profile: { borderWidth: 1, borderRadius: 16, padding: 16, gap: 4 },
  profileTitle: { fontSize: 17, fontWeight: '700' },
  profileDescription: { fontSize: 13, lineHeight: 18 },
  panel: { borderWidth: 1, borderRadius: 20, padding: 18, gap: 14 },
  panelRow: { flexDirection: 'row', alignItems: 'center', gap: 12 },
  panelCopy: { flex: 1, gap: 3 },
  panelTitle: { fontSize: 19, fontWeight: '700' },
  panelDetail: { fontSize: 13, lineHeight: 18 },
  track: { height: 7, overflow: 'hidden', borderRadius: 99 },
  fill: { height: '100%', borderRadius: 99 },
  progressText: { fontSize: 12, fontVariant: ['tabular-nums'] },
  primaryButton: {
    minHeight: 50,
    borderRadius: 14,
    justifyContent: 'center',
    alignItems: 'center',
  },
  primaryButtonText: { color: '#FFFFFF', fontSize: 16, fontWeight: '800' },
  message: { borderWidth: 1, borderRadius: 16, padding: 16, gap: 6 },
  messageTitle: { fontSize: 16, fontWeight: '800' },
  messageBody: { fontSize: 13, lineHeight: 19 },
  results: { gap: 10 },
  resultsHeader: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    marginBottom: 2,
  },
  resultsTitle: { fontSize: 24, fontWeight: '800' },
  resultsMeta: { fontSize: 13, marginTop: 3 },
  shareButton: {
    borderWidth: 1,
    borderRadius: 99,
    paddingHorizontal: 14,
    paddingVertical: 9,
  },
  shareText: { fontSize: 13, fontWeight: '700' },
  metric: { borderWidth: 1, borderRadius: 14, padding: 14, gap: 5 },
  metricName: {
    fontSize: 13,
    fontWeight: '700',
    fontFamily: Platform.select({ ios: 'Menlo', android: 'monospace' }),
  },
  metricVariant: { fontSize: 12, lineHeight: 17 },
  metricNumbers: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    marginTop: 7,
  },
  startupMetrics: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    columnGap: 28,
    rowGap: 12,
  },
  secondaryButton: {
    minHeight: 44,
    borderWidth: 1,
    borderRadius: 14,
    justifyContent: 'center',
    alignItems: 'center',
  },
  renderProbe: {
    position: 'absolute',
    width: 320,
    height: 1,
    opacity: 0,
    overflow: 'hidden',
  },
  renderProbeText: { fontSize: 12, lineHeight: 16 },
  metricLabel: { fontSize: 10, textTransform: 'uppercase', letterSpacing: 0.8 },
  metricValue: {
    fontSize: 14,
    fontWeight: '700',
    fontVariant: ['tabular-nums'],
    marginTop: 2,
  },
  note: { borderTopWidth: 1, paddingTop: 16, gap: 5 },
  noteTitle: { fontSize: 14, fontWeight: '700' },
  noteBody: { fontSize: 12, lineHeight: 18 },
  sourceLink: { fontSize: 12, fontWeight: '700', marginTop: 4 },
  comparisonTitle: { fontSize: 13, fontWeight: '800', marginTop: 2 },
  referenceNote: { fontSize: 11, lineHeight: 16 },
  referenceLibrary: { fontWeight: '700' },
  referenceRow: { fontSize: 11, lineHeight: 18, fontVariant: ['tabular-nums'] },
});

export default App;
