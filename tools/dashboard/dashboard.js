/* Shared reducers are also tested with node --test; rendering has no dependencies. */
(function (root) {
  'use strict';
  const METRICS = {
    deliveries_per_second: {unit: 'msg/s', higher: true, note: 'Higher is better. Medians of checked trial throughput; each case uses its own scale.'},
    p50_us: {unit: 'µs', note: 'Lower is better. Median of trial p50 values. Read each case’s latency boundary.'},
    p95_us: {unit: 'µs', note: 'Lower is better. Median of trial p95 values, not a pooled p95.'},
    p99_us: {unit: 'µs', note: 'Lower is better. Median of trial p99 values, not a pooled p99.'},
    fetch_p50_us: {unit: 'µs', note: 'Lower is better. Fetch(1) request to delivery, including the consumer reservation.'},
    fetch_p99_us: {unit: 'µs', note: 'Lower is better. Median of trial Fetch(1) p99 values.'},
    cycle_p99_us: {unit: 'µs', note: 'Lower is better. Median of trial p99 values for Fetch(1) through confirmed ACK.'},
    durable_p99_us: {unit: 'µs', note: 'Lower is better. Median of trial publish-to-durable-ACK p99 values.'},
    cycle_p50_us: {unit: 'µs', note: 'Lower is better. Fetch(1) request through confirmed ACK, including client validation.'},
    durable_p50_us: {unit: 'µs', note: 'Lower is better. Durable publish ACK latency, measured separately from the live Core callback.'},
    server_cpu_seconds: {unit: 's', note: 'Lower is better. Server CPU includes setup, prefill, validation and teardown, not only the timed workload.'},
    server_rss_kib: {unit: 'MiB', note: 'Lower is better. Final resident memory; this is not peak RSS.'}
  };
  const SERIES = {
    screech: ['Screech · FULL', '#087f75'], 'screech-unsafe-ack': ['Screech · UNSAFE ACK durability', '#ad301c'], 'screech-before': ['Screech · prior', '#697782'],
    'nats-main': ['NATS · always', '#7060b6'], 'nats-default': ['NATS · default 2m', '#a66500'],
    'nats-sync-10ms': ['NATS · 10 ms', '#376dae']
  };
  const CASES = {
    fanout: 'Core fanout', wildcard: 'Core wildcard · one subject', queue: 'Core queue', request: 'Core request / reply',
    'core-subject-churn': 'Core subject churn', 'core-many-subscriptions': 'Core many subscriptions', 'core-churn-many-subscriptions': 'Core churn + many subscriptions',
    'core-stream': 'Core on a durable stream', durable: 'Durable publish', dedup: 'Publish deduplication',
    'consume-ack-window': 'Fetch(1000) + 16 ACK clients', consume: 'Fetch(128) + ACK', 'consume-limits': 'Fetch(128) + ACK · limits',
    'consume-one': 'Fetch(1) + ACK', 'consume-next': 'ACK + next', 'consume-parallel': 'Fetch(128) + 16 ACK clients',
    'durable-parallel': '16 durable publishers', 'consume-one-parallel': '16 Fetch(1) + ACK clients',
    'mixed-parallel': '8 publishers + 8 consumers', 'mixed-streams': 'Mixed · four streams'
  };
  const coreOnly = scenario => ['fanout', 'wildcard', 'queue', 'request', 'core-subject-churn', 'core-many-subscriptions', 'core-churn-many-subscriptions'].includes(scenario);
  function seriesName(id, scenario) {
    if (coreOnly(scenario)) return id.startsWith('nats-') ? 'NATS · Core' : id === 'screech' ? 'Screech · Core' : 'Screech · prior Core';
    return (SERIES[id] || [id.replace('nats-sync-', 'NATS · ')])[0];
  }
  function median(values) {
    if (!values.length) return null;
    const a = [...values].sort((a, b) => a - b), mid = Math.floor(a.length / 2);
    return a.length % 2 ? a[mid] : (a[mid - 1] + a[mid]) / 2;
  }
  function scope(row, metric) {
    if (metric?.startsWith('fetch_')) return 'Fetch(1) request to delivery';
    if (metric?.startsWith('cycle_')) return 'Fetch(1) through confirmed ACK';
    if (metric?.startsWith('durable_')) return 'publish to durable ACK';
    if (row.latency_scope) return row.latency_scope;
    if (['consume', 'consume-one', 'consume-limits', 'consume-parallel', 'consume-ack-window'].includes(row.scenario)) return 'confirmed ACK only; Fetch excluded from latency';
    if (row.scenario === 'consume-next') return 'ACK + next round trip; final ACK only';
    if (row.scenario === 'request') return 'request / reply round trip';
    if (['durable', 'dedup'].includes(row.scenario)) return 'publish to durable ACK';
    return 'throughput only; no latency samples';
  }
  function summarize(rows, metric) {
    if (!METRICS[metric]) throw Error('Unknown metric');
    const groups = new Map();
    for (const row of rows) {
      if (row.checked !== true) throw Error('Unchecked trial');
      const key = JSON.stringify([row.scenario, row.payload_bytes, row.subscribers, row.publishes]);
      if (!groups.has(key)) groups.set(key, {key, row, title: CASES[row.scenario] || row.scenario, series: new Map()});
      const group = groups.get(key);
      if (!group.series.has(row.server)) group.series.set(row.server, []);
      const v = row[metric];
      if (typeof v === 'number' && Number.isFinite(v) && v >= 0) group.series.get(row.server).push(metric === 'server_rss_kib' ? v / 1024 : v);
    }
    return [...groups.values()].map(g => ({...g, series: new Map([...g.series].map(([id, vs]) => [id, {
      median: median(vs), min: vs.length ? Math.min(...vs) : null, max: vs.length ? Math.max(...vs) : null, trials: vs.length
    }]))}));
  }
  function comparison(value, base, higher) {
    if (value === null || base === null || value <= 0 || base <= 0) return null;
    const factor = higher ? value / base : base / value;
    return {factor, better: factor >= 1, text: factor >= 1 ? `${factor.toFixed(2)}× ${higher ? 'throughput' : 'lower'}` : `${(1 / factor).toFixed(2)}× ${higher ? 'less throughput' : 'higher'}`};
  }
  const api = {METRICS, median, scope, summarize, comparison, seriesName};
  if (typeof module !== 'undefined') module.exports = api;
  if (!root.document) return;
  const $ = id => document.getElementById(id), data = root.BENCHMARK_DATA;
  function el(tag, text, cls) {
    const node = document.createElement(tag);
    if (text !== undefined) node.textContent = text;
    if (cls) node.className = cls;
    return node;
  }
  function link(node, url) {
    try { const u = new URL(url); if (['https:', 'http:'].includes(u.protocol)) { node.href = u.href; return; } } catch (_) {}
    node.hidden = true;
  }
  if (!data || !data.campaigns || !data.campaigns.length) { $('summary').textContent = 'No benchmark campaigns available.'; return; }
  const params = new URLSearchParams(location.search);
  const state = {metric: METRICS[params.get('metric')] ? params.get('metric') : 'deliveries_per_second',
    baseline: params.get('baseline') || 'nats-main', search: params.get('search') || '', table: params.get('view') === 'table', visible: new Set()};
  for (const c of data.campaigns) { const o = el('option', c.label); o.value = c.id; $('campaign').append(o); }
  $('campaign').value = data.campaigns.some(c => c.id === params.get('campaign')) ? params.get('campaign') : data.campaigns[0].id;
  $('metric').value = state.metric; $('search').value = state.search;
  $('generated').textContent = `Built ${new Date(data.generated_at).toLocaleString(undefined, {dateStyle: 'medium', timeStyle: 'short'})}`;
  $('revision').textContent = data.revision ? `Site commit ${data.revision.slice(0, 9)} ↗` : 'Local preview';
  if (data.revision) link($('revision'), `${data.repository}/commit/${data.revision}`);
  link($('run-link'), data.run_url); link($('source'), data.repository);
  const campaign = () => data.campaigns.find(c => c.id === $('campaign').value);
  const nf = new Intl.NumberFormat(undefined, {maximumFractionDigits: 2});
  function format(v) { return v === null ? '—' : new Intl.NumberFormat(undefined, {maximumFractionDigits: state.metric === 'deliveries_per_second' ? 0 : state.metric === 'server_cpu_seconds' ? 3 : 2}).format(v); }
  function seriesControls() {
    const ids = [...new Set(campaign().rows.map(r => r.server))].sort((a,b) => (a === 'screech' ? -1 : b === 'screech' ? 1 : a.localeCompare(b)));
    $('baseline').replaceChildren(); $('servers').replaceChildren(el('legend', 'Show series'));
    state.visible = new Set(ids);
    for (const id of ids) {
      const name = seriesName(id), color = (SERIES[id] || ['', '#697782'])[1];
      const option = el('option', name); option.value = id; $('baseline').append(option);
      const label = el('label'); label.style.setProperty('--series', color);
      const input = el('input'); input.type = 'checkbox'; input.checked = true; input.value = id;
      input.addEventListener('change', () => { if (input.checked) state.visible.add(id); else state.visible.delete(id); render(); });
      label.append(input, document.createTextNode(name)); $('servers').append(label);
    }
    if (!ids.includes(state.baseline)) state.baseline = ids.includes('nats-main') ? 'nats-main' : ids[0];
    $('baseline').value = state.baseline;
  }
  function render() {
    const c = campaign(), meta = METRICS[state.metric];
    const groups = summarize(c.rows, state.metric).filter(g => {
      const text = `${g.title} ${g.row.scenario} ${g.row.payload_bytes} ${g.row.publishes} ${g.row.core_subjects ?? ''} ${g.row.unused_wildcards ?? ''}`.toLowerCase();
      return text.includes(state.search.toLowerCase()) && [...g.series].some(([id,v]) => state.visible.has(id) && v.median !== null);
    });
    $('charts').replaceChildren(); $('table-body').replaceChildren();
    $('summary').textContent = `${groups.length} cases · ${c.rows.length} checked trials in this campaign · ${meta.higher ? 'higher' : 'lower'} is better`;
    $('metric-note').textContent = meta.note; $('value-heading').textContent = `Median · ${meta.unit}`;
    $('environment').textContent = JSON.stringify(c.environment, null, 2); $('raw').href = c.download;
    for (const g of groups) {
      const r = g.row, selected = [...g.series].filter(([id]) => state.visible.has(id));
      const card = el('article', undefined, 'case'), top = el('div', undefined, 'case-top'); top.append(el('h2', g.title));
      const own = g.series.get('screech'), base = g.series.get(state.baseline);
      const cmp = comparison(own ? own.median : null, base ? base.median : null, meta.higher);
      if (cmp && state.baseline !== 'screech') {
        const badge = el('span', cmp.text, `badge${cmp.better ? '' : ' slower'}`); badge.title = `Screech vs ${seriesName(state.baseline,r.scenario)}`; top.append(badge);
      }
      const clients = r.consumer_connections !== undefined ? ` · ${r.publishers} publishers · ${r.consumer_connections} consumers · ${r.streams || 1} stream(s)` : coreOnly(r.scenario) || r.scenario === 'core-stream' ? ` · ${r.subscribers} subscriber(s)` : '';
      const topology = r.core_subjects !== undefined ? ` · ${nf.format(r.core_subjects)} subject(s) · ${nf.format(r.unused_wildcards)} nonmatching wildcard subscriptions per subscriber` : '';
      const details = `${nf.format(r.payload_bytes)} B · ${nf.format(r.publishes)} publishes${clients}${topology}`;
      card.append(top, el('p', details, 'case-details'));
      const max = Math.max(...selected.map(([,s]) => s.median || 0), 1);
      for (const [id, s] of selected) {
        const name = seriesName(id, r.scenario), color = (SERIES[id] || ['', '#697782'])[1];
        const row = el('div', undefined, 'bar-row'); row.style.setProperty('--series', color);
        const track = el('div', undefined, 'bar-track'), bar = el('div', undefined, 'bar');
        if (s.median === null) { bar.hidden = true; } else bar.style.width = `${s.median / max * 100}%`;
        track.append(bar); row.append(el('span', name, 'series-label'), track, el('span', format(s.median), 'value'));
        row.title = `${name}: ${format(s.median)} ${meta.unit}; range ${format(s.min)}–${format(s.max)}; ${s.trials} trials`; card.append(row);
        const tr = el('tr'); for (const text of [`${g.title} · ${details}`, name, `${format(s.median)} ${meta.unit}`, `${format(s.min)}–${format(s.max)}`, s.trials]) tr.append(el('td', text));
        $('table-body').append(tr);
      }
      card.append(el('p', `Latency: ${scope(r, state.metric)}. ${meta.unit}. Observed trial ranges are available on each bar.`, 'case-note'));
      $('charts').append(card);
    }
    $('empty').hidden = groups.length > 0; $('charts').hidden = state.table; $('table-view').hidden = !state.table;
    $('charts-button').setAttribute('aria-pressed', String(!state.table)); $('table-button').setAttribute('aria-pressed', String(state.table));
    const url = new URL(location.href); for (const [k,v] of Object.entries({campaign:c.id,metric:state.metric,baseline:state.baseline,search:state.search,view:state.table ? 'table' : 'charts'})) url.searchParams.set(k,v);
    history.replaceState(null, '', url);
  }
  $('campaign').addEventListener('change', () => { seriesControls(); render(); });
  $('metric').addEventListener('change', () => { state.metric = $('metric').value; render(); });
  $('baseline').addEventListener('change', () => { state.baseline = $('baseline').value; render(); });
  $('search').addEventListener('input', () => { state.search = $('search').value; render(); });
  for (const table of [false,true]) $(table ? 'table-button' : 'charts-button').addEventListener('click', () => { state.table = table; render(); });
  seriesControls(); render();
})(typeof window !== 'undefined' ? window : globalThis);
