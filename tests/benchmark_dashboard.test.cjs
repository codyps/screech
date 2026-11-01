const {test} = require('node:test');
const assert = require('node:assert/strict');
const {summarize, comparison, scope, median, seriesName} = require('../tools/dashboard/dashboard.js');
const trial = (server, value, extra={}) => ({server, scenario:'consume-one-parallel', payload_bytes:128, subscribers:1, publishes:20000, checked:true, p50_us:value, ...extra});
test('medians use trial percentiles, not a pooled latency distribution', () => {
  const cases = summarize([trial('screech', 100), trial('screech', 9), trial('screech', 11)], 'p50_us');
  assert.deepEqual(cases[0].series.get('screech'), {median:11, min:9, max:100, trials:3});
  assert.equal(median([2,8]), 5);
});
test('workload size, payload and recipient count remain separate', () => {
  const rows=[trial('screech',1),trial('nats-main',2),trial('screech',1,{publishes:5000}),trial('screech',1,{payload_bytes:1024}),trial('screech',1,{subscribers:4})];
  assert.equal(summarize(rows,'p50_us').length,4);
});
test('missing latency stays missing; invalid numerical data cannot become zero', () => {
  const s=summarize([trial('screech',null),trial('screech',undefined),trial('nats-main',NaN)],'p50_us')[0].series;
  assert.deepEqual(s.get('screech'),{median:null,min:null,max:null,trials:0});
  assert.equal(s.get('nats-main').median,null);
  assert.equal(comparison(null,2,true),null);
});
test('throughput and latency comparisons have opposite directions', () => {
  assert.equal(comparison(200,100,true).factor,2);
  assert.equal(comparison(50,100,false).factor,2);
  assert.equal(comparison(200,100,false).better,false);
  assert.equal(comparison(0,100,false),null);
});
test('memory converts KiB to MiB once', () => {
  const s=summarize([trial('screech',1,{server_rss_kib:2048})],'server_rss_kib')[0].series.get('screech');
  assert.equal(s.median,2);
});
test('consumer ACK-only scope and concurrent cycle scope are distinct', () => {
  assert.match(scope({scenario:'consume-one'}),/Fetch excluded/);
  assert.equal(scope({latency_scope:'fetch-one-through-confirmed-ack'}),'fetch-one-through-confirmed-ack');
  assert.equal(seriesName('nats-main','fanout'),'NATS · Core');
});
test('unchecked rows and unknown metrics fail closed', () => {
  assert.throws(()=>summarize([trial('screech',1,{checked:false})],'p50_us'));
  assert.throws(()=>summarize([], 'bogus'));
});
