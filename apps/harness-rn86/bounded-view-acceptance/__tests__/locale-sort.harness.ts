import { expect, test } from 'react-native-harness';
import { Platform } from 'react-native';

test('compares repeated localeCompare options with one reusable collator', async () => {
  const hermes = (
    globalThis as unknown as {
      HermesInternal?: { getRuntimeProperties: () => unknown };
    }
  ).HermesInternal;
  expect(hermes !== undefined).toBe(true);
  const notify = async (kind: string, value: unknown) => {
    const response = await fetch(`http://127.0.0.1:19062/${kind}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(value),
    });
    expect(response.ok).toBe(true);
  };
  await notify('ready', { phase: 'locale-sort', at: Date.now() });
  const started = performance.now();
  const constructionStart = performance.now();
  const collator = new Intl.Collator(undefined, {
    numeric: true,
    sensitivity: 'base',
  });
  const constructionMs = performance.now() - constructionStart;
  const stems = [
    'Apfel',
    'äpfel',
    'ÄPFEL',
    'café',
    'Cafe',
    'Öl',
    'ol',
    'Zitrone',
    'über',
  ];
  const titles = Array.from({ length: 54 }, (_, i) => ({
    id: i,
    title: `${stems[i % stems.length]} ${(i * 17) % 31}`,
  }));
  const baseline = (a: (typeof titles)[number], b: (typeof titles)[number]) =>
    a.title.localeCompare(b.title, undefined, {
      numeric: true,
      sensitivity: 'base',
    });
  const candidate = (a: (typeof titles)[number], b: (typeof titles)[number]) =>
    collator.compare(a.title, b.title);
  let signsChecked = 0;
  for (const left of titles)
    for (const right of titles) {
      expect(Math.sign(baseline(left, right))).toBe(
        Math.sign(candidate(left, right)),
      );
      signsChecked++;
    }
  const expected = titles
    .slice()
    .sort(baseline)
    .map(x => x.id);
  expect(
    titles
      .slice()
      .sort(candidate)
      .map(x => x.id),
  ).toEqual(expected);
  titles.slice().sort(baseline);
  titles.slice().sort(candidate);
  const rounds: unknown[] = [];
  for (let round = 0; round < 5; round++) {
    const result: Record<string, unknown> = {
      round,
      order: round % 2 ? ['candidate', 'baseline'] : ['baseline', 'candidate'],
    };
    for (const method of result.order as string[]) {
      const input = titles.slice();
      const begin = performance.now();
      input.sort(method === 'baseline' ? baseline : candidate);
      result[method + 'Ms'] = performance.now() - begin;
      expect(input.map(x => x.id)).toEqual(expected);
    }
    rounds.push(result);
    if (performance.now() - started > 60000)
      throw new Error('bounded benchmark exceeded 60 seconds');
  }
  await notify('report', {
    phase: 'locale-sort',
    synthetic: true,
    productLatencyProved: false,
    hermes: hermes?.getRuntimeProperties(),
    reactNative: Platform.constants.reactNativeVersion,
    titles: titles.length,
    signsChecked,
    sortedOrderParity: true,
    resolvedOptions: collator.resolvedOptions(),
    constructionMs,
    rounds,
    totalMs: performance.now() - started,
  });
  await notify('closed', {
    phase: 'locale-sort',
    synthetic: true,
    at: Date.now(),
  });
});
