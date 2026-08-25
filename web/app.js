const $ = (selector) => document.querySelector(selector);
const message = (text, bad = false) => {
  $('#message').textContent = text;
  $('#message').style.color = bad ? '#ff9bad' : '#5ee7b5';
};
async function request(path, options = {}) {
  const response = await fetch(path, {
    headers: options.body ? {'Content-Type': 'application/json'} : {},
    ...options,
  });
  const value = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(value.error || `HTTP ${response.status}`);
  return value;
}
async function refresh() {
  try {
    const [metrics, values] = await Promise.all([request('/api/v1/metrics'), request('/api/v1/kv')]);
    const names = {
      recordsWritten: '累计写入', bytesWritten: '写入字节', fsyncs: 'fsync 次数',
      segments: '日志段', durableSeq: '持久水位', checksumFailures: '校验失败',
    };
    $('#metrics').innerHTML = Object.entries(names).map(([key, name]) =>
      `<div class="metric"><span>${name}</span><strong>${metrics[key] ?? 0}</strong></div>`).join('');
    $('#items').className = values.items.length ? 'list' : 'list empty';
    $('#items').innerHTML = values.items.length ? values.items.map(item =>
      `<div class="entry"><b>${escapeHTML(item.key)}</b><span>${escapeHTML(item.value)}</span></div>`).join('') : '暂无数据';
  } catch (error) {
    $('#status').textContent = '● 连接失败';
    message(error.message, true);
  }
}
function escapeHTML(value) {
  const node = document.createElement('span');
  node.textContent = value;
  return node.innerHTML;
}
$('#write-form').addEventListener('submit', async (event) => {
  event.preventDefault();
  try {
    const result = await request('/api/v1/write', {method: 'POST', body: JSON.stringify({key: $('#key').value, value: $('#value').value})});
    message(`记录 #${result.seq} 已写入并刷盘。`); await refresh();
  } catch (error) { message(error.message, true); }
});
$('#batch').addEventListener('click', async () => {
  try {
    const count = Number($('#batch-count').value);
    const result = await request('/api/v1/write/batch', {method: 'POST', body: JSON.stringify({count})});
    message(`${result.seqs.length} 条记录已通过组提交持久化。`); await refresh();
  } catch (error) { message(error.message, true); }
});
$('#sync').addEventListener('click', async () => {
  try { const r = await request('/api/v1/sync', {method: 'POST'}); message(`持久水位：${r.durableSeq}`); await refresh(); }
  catch (error) { message(error.message, true); }
});
$('#snapshot').addEventListener('click', async () => {
  try { const r = await request('/api/v1/snapshot', {method: 'POST'}); message(`快照已保存到 #${r.snapshotSeq}`); }
  catch (error) { message(error.message, true); }
});
$('#load-log').addEventListener('click', async () => {
  try {
    const result = await request('/api/v1/log?from=1&limit=100');
    $('#records').className = result.records.length ? 'list' : 'list empty';
    $('#records').innerHTML = result.records.length ? result.records.slice(-30).reverse().map(record =>
      `<div class="entry"><b>#${record.seq}</b><span>segment ${record.segment} · offset ${record.offset}</span></div>`).join('') : '暂无记录';
  } catch (error) { message(error.message, true); }
});
$('#corrupt').addEventListener('click', async () => {
  try { const seq = Number($('#corrupt-seq').value); await request(`/api/v1/corrupt?seq=${seq}`, {method: 'POST'}); message(`记录 #${seq} 已被篡改。`, true); }
  catch (error) { message(error.message, true); }
});
$('#recover').addEventListener('click', async () => {
  try { const r = await request('/api/v1/recover', {method: 'POST'}); message(`恢复完成，最后序号 ${r.recovery.lastSeq}。`); await refresh(); }
  catch (error) { message(error.message, true); }
});
$('#crash').addEventListener('click', async () => {
  try { const r = await request('/api/v1/crash', {method: 'POST'}); message(r.message, true); }
  catch (error) { message(error.message, true); }
});
$('#refresh').addEventListener('click', refresh);
refresh();
setInterval(refresh, 3000);
