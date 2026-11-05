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
    server_cpu_seconds: {unit: 's', note: 'Lower is better. Server CPU sums clustered voters and includes setup, prefill, validation and teardown, not only the timed workload.'},
    server_rss_kib: {unit: 'MiB', note: 'Lower is better. Final resident memory, summed across clustered voters; this is not peak RSS.'}
  };
  const SERIES = {
    screech: ['Screech · FULL', '#087f75'], 'screech-unsafe-ack': ['Screech · UNSAFE ACK durability', '#ad301c'], 'screech-before': ['Screech · prior', '#697782'],
    'screech-cluster-3': ['Screech · R3 · FULL', '#1679a0'],
    'screech-cluster-5': ['Screech · R5 · FULL', '#244b74'],
    'nats-cluster-3-always': ['NATS · R3 · always', '#a35b9c'],
    'nats-cluster-5-always': ['NATS · R5 · always', '#58407c'],
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
      const key = JSON.stringify([workloadKey(row), row.publishes, row.voters || 1, row.stream_replicas || 1,
        row._campaign || '', metric.endsWith('_us') ? scope(row, metric) : '']);
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
  function workloadKey(row) {
    return JSON.stringify([row.scenario, row.payload_bytes, row.subscribers,
      ...['core_subjects','unused_wildcards','core_window','publishers','consumer_connections','streams'].map(k => row[k] ?? null)]);
  }
  function campaignRows(campaigns, selected = 'all') {
    return campaigns.filter(c => selected === 'all' || c.id === selected)
      .flatMap(c => c.rows.map(row => ({...row, _campaign: c.id, _campaign_label: c.label})));
  }
  function comparisonPeer(id, scenario) {
    if (coreOnly(scenario)) return id.startsWith('nats-') ? 'screech' : 'nats-main';
    const cluster = id.match(/^(screech|nats)-cluster-([35])(?:-always)?$/);
    if (cluster) return cluster[1] === 'screech' ? `nats-cluster-${cluster[2]}-always` : `screech-cluster-${cluster[2]}`;
    if (id.startsWith('screech')) return 'nats-main';
    return id === 'nats-main' ? 'screech' : id.startsWith('nats-') ? 'screech-unsafe-ack' : null;
  }
  function tiles(rows, metric) {
    const result = new Map();
    for (const group of summarize(rows, metric)) {
      const key = workloadKey(group.row);
      if (!result.has(key)) result.set(key, {key, title: group.title, row: group.row, results: []});
      for (const [id, stats] of group.series) result.get(key).results.push({id, ...stats, row: group.row, peers: group.series});
    }
    return [...result.values()];
  }
  function comparison(value, base, higher, metric = higher ? 'deliveries_per_second' : 'p99_us') {
    if (!Number.isFinite(value) || !Number.isFinite(base) || value <= 0 || base <= 0) return null;
    const factor = higher ? value / base : base / value;
    const resource = metric === 'server_cpu_seconds' ? 'CPU' : metric === 'server_rss_kib' ? 'memory' : null;
    return {factor, better: factor >= 1, text: factor === 1 ? '1.00× · equal' : resource
      ? `${(factor >= 1 ? factor : 1 / factor).toFixed(2)}× ${factor >= 1 ? 'less' : 'more'} ${resource}`
      : `${(factor >= 1 ? factor : 1 / factor).toFixed(2)}× ${factor >= 1 ? 'faster' : 'slower'}`};
  }
  const api = {METRICS, median, scope, summarize, tiles, campaignRows, comparisonPeer, comparison, seriesName};
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
    baseline: params.get('baseline') || 'auto', search: params.get('search') || '', table: params.get('view') === 'table',
    hiddenSeries: new Set((params.get('hide') || '').split(',').filter(Boolean)),
    hiddenCases: new Set((params.get('hideCases') || '').split(',').filter(Boolean))};
  const allOption = el('option', 'All benchmarks'); allOption.value = 'all'; $('campaign').append(allOption);
  for (const c of data.campaigns) { const o = el('option', c.label); o.value = c.id; $('campaign').append(o); }
  $('campaign').value = data.campaigns.some(c => c.id === params.get('campaign')) ? params.get('campaign') : 'all';
  $('metric').value = state.metric; $('search').value = state.search;
  $('generated').textContent = `Built ${new Date(data.generated_at).toLocaleString(undefined, {dateStyle: 'medium', timeStyle: 'short'})}`;
  $('revision').textContent = data.revision ? `Site commit ${data.revision.slice(0, 9)} ↗` : 'Local preview';
  if (data.revision) link($('revision'), `${data.repository}/commit/${data.revision}`);
  link($('run-link'), data.run_url); link($('source'), data.repository);
  const campaigns = () => data.campaigns.filter(c => $('campaign').value === 'all' || c.id === $('campaign').value);
  const nf = new Intl.NumberFormat(undefined, {maximumFractionDigits: 2});
  function format(v) { return v === null ? '—' : new Intl.NumberFormat(undefined, {maximumFractionDigits: state.metric === 'deliveries_per_second' ? 0 : state.metric === 'server_cpu_seconds' ? 3 : 2}).format(v); }
  const order = ['screech','nats-main','screech-unsafe-ack','nats-sync-10ms','nats-default','screech-before',
    'screech-cluster-3','nats-cluster-3-always','screech-cluster-5','nats-cluster-5-always'];
  function seriesOrder(id) { const i = order.indexOf(id); return i < 0 ? order.length : i; }
  function checkboxes(target, items, hidden, name) {
    $(target).replaceChildren(el('legend', name));
    const buttons = el('div', undefined, 'filter-actions');
    for (const [text, hide] of [['All', false], ['None', true]]) {
      const button = el('button', text); button.type = 'button'; button.setAttribute('aria-label', `${text} ${name.toLowerCase()}`);
      button.addEventListener('click', () => {
        for (const [id] of items) hide ? hidden.add(id) : hidden.delete(id);
        for (const input of $(target).querySelectorAll('input')) input.checked = !hidden.has(input.value);
        render();
      });
      buttons.append(button);
    }
    $(target).append(buttons);
    for (const [id, text, color] of items) {
      const label = el('label'); if (color) label.style.setProperty('--series', color);
      const input = el('input'); input.type = 'checkbox'; input.checked = !hidden.has(id); input.value = id;
      input.addEventListener('change', () => { input.checked ? hidden.delete(id) : hidden.add(id); render(); });
      label.append(input, document.createTextNode(text)); $(target).append(label);
    }
  }
  function seriesControls() {
    const rows = campaignRows(campaigns()), ids = [...new Set(rows.map(r => r.server))].sort((a,b) => seriesOrder(a) - seriesOrder(b) || a.localeCompare(b));
    const automatic = el('option', 'Matching peer · automatic'); automatic.value = 'auto'; $('baseline').replaceChildren(automatic);
    for (const id of ids) {
      const option = el('option', seriesName(id)); option.value = id; $('baseline').append(option);
    }
    if (state.baseline !== 'auto' && !ids.includes(state.baseline)) state.baseline = 'auto';
    $('baseline').value = state.baseline;
    checkboxes('servers', ids.map(id => [id, seriesName(id), (SERIES[id] || ['', '#697782'])[1]]), state.hiddenSeries, 'Server modes');
    const cases = [...new Set(rows.map(r => r.scenario))];
    checkboxes('workloads', cases.map(id => [id, CASES[id] || id]), state.hiddenCases, 'Workloads');
  }
  function render() {
    const active = campaigns(), rows = campaignRows(active), meta = METRICS[state.metric];
    const groups = tiles(rows, state.metric).filter(g => {
      const text = `${g.title} ${g.row.scenario} ${g.row.payload_bytes} ${g.results.map(r => r.row.publishes).join(' ')} ${g.row.core_subjects ?? ''} ${g.row.unused_wildcards ?? ''}`.toLowerCase();
      return !state.hiddenCases.has(g.row.scenario) && text.includes(state.search.toLowerCase())
        && g.results.some(r => !state.hiddenSeries.has(r.id) && r.median !== null);
    });
    $('charts').replaceChildren(); $('table-body').replaceChildren();
    $('summary').textContent = `${groups.length} workloads · ${rows.length} checked trials in ${active.length} campaign(s) · ${meta.higher ? 'higher' : 'lower'} is better`;
    $('metric-note').textContent = meta.note; $('value-heading').textContent = `Median · ${meta.unit}`;
    $('environment').textContent = JSON.stringify(active.map(c => ({campaign: c.label, environment: c.environment})), null, 2);
    $('raw-downloads').replaceChildren();
    for (const c of active) { const a = el('a', `${c.label} · raw JSON ↓`); a.href = c.download; $('raw-downloads').append(a); }
    for (const g of groups) {
      const r = g.row, selected = g.results.filter(s => !state.hiddenSeries.has(s.id))
        .sort((a,b) => seriesOrder(a.id) - seriesOrder(b.id) || a.id.localeCompare(b.id) || a.row.publishes - b.row.publishes || a.row._campaign.localeCompare(b.row._campaign));
      const card = el('article', undefined, 'case'), top = el('div', undefined, 'case-top'); top.append(el('h2', g.title));
      const clients = r.consumer_connections !== undefined ? ` · ${r.publishers} publishers · ${r.consumer_connections} consumers · ${r.streams || 1} stream(s)` : coreOnly(r.scenario) || r.scenario === 'core-stream' ? ` · ${r.subscribers} subscriber(s)` : '';
      const topology = r.core_subjects !== undefined ? ` · ${nf.format(r.core_subjects)} subject(s) · ${nf.format(r.unused_wildcards)} nonmatching wildcard subscriptions per subscriber` : '';
      const details = `${nf.format(r.payload_bytes)} B${clients}${topology}`;
      card.append(top, el('p', details, 'case-details'));
      const max = Math.max(...selected.map(s => s.median || 0), 1);
      for (const s of selected) {
        const id = s.id;
        const name = seriesName(id, r.scenario), color = (SERIES[id] || ['', '#697782'])[1];
        const result = el('div', undefined, 'result'), row = el('div', undefined, 'bar-row'); row.style.setProperty('--series', color);
        const track = el('div', undefined, 'bar-track'), bar = el('div', undefined, 'bar');
        if (s.median === null) { bar.hidden = true; } else bar.style.width = `${s.median / max * 100}%`;
        track.append(bar); row.append(el('span', name, 'series-label'), track, el('span', format(s.median), 'value'));
        const samples = `${nf.format(s.row.publishes)} publishes · ${s.trials} trials`;
        row.title = `${name}: ${format(s.median)} ${meta.unit}; range ${format(s.min)}–${format(s.max)}; ${samples}; ${s.row._campaign_label}`;
        const peer = state.baseline === 'auto' ? comparisonPeer(id, r.scenario) : state.baseline;
        const base = s.peers.get(peer), cmp = comparison(s.median, base?.median ?? null, meta.higher, state.metric);
        const peerName = peer ? seriesName(peer, r.scenario) : 'matching peer';
        const comparisonText = s.median === null ? '— Not measured' : peer === id ? 'Selected baseline'
          : cmp ? `${cmp.text} vs ${peerName}` : `— No matched ${peerName} result`;
        const foot = el('div', undefined, 'result-meta');
        foot.append(el('span', samples, 'sample-count'), el('span', comparisonText,
          `comparison${!cmp ? ' unavailable' : !cmp.better ? ' slower' : ''}`));
        result.title = row.title; result.append(row, foot); card.append(result);
        const tr = el('tr');
        for (const text of [`${g.title} · ${details}`, name, `${samples} · ${s.row._campaign_label}`, `${format(s.median)} ${meta.unit}`, `${format(s.min)}–${format(s.max)}`, comparisonText]) tr.append(el('td', text));
        $('table-body').append(tr);
      }
      const boundaries = [...new Set(selected.map(s => scope(s.row, state.metric)))];
      card.append(el('p', `Latency: ${boundaries.join('; ')}. ${meta.unit}. Ratios use the same campaign, publication count and replication count, even when the peer is hidden.`, 'case-note'));
      $('charts').append(card);
    }
    $('empty').hidden = groups.length > 0; $('charts').hidden = state.table; $('table-view').hidden = !state.table;
    $('charts-button').setAttribute('aria-pressed', String(!state.table)); $('table-button').setAttribute('aria-pressed', String(state.table));
    const url = new URL(location.href); for (const [k,v] of Object.entries({campaign:$('campaign').value,metric:state.metric,baseline:state.baseline,search:state.search,view:state.table ? 'table' : 'charts',
      hide:[...state.hiddenSeries].sort().join(','), hideCases:[...state.hiddenCases].sort().join(',')})) {
      if (v) url.searchParams.set(k,v); else url.searchParams.delete(k);
    }
    history.replaceState(null, '', url);
  }
  $('campaign').addEventListener('change', () => { seriesControls(); render(); });
  $('metric').addEventListener('change', () => { state.metric = $('metric').value; render(); });
  $('baseline').addEventListener('change', () => { state.baseline = $('baseline').value; render(); });
  $('search').addEventListener('input', () => { state.search = $('search').value; render(); });
  for (const table of [false,true]) $(table ? 'table-button' : 'charts-button').addEventListener('click', () => { state.table = table; render(); });
  seriesControls(); render();
})(typeof window !== 'undefined' ? window : globalThis);
