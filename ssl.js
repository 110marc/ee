/* ssl.js — SSL certificate checker (no common.js dependency — no host selector) */
'use strict';

var _data = null;

function el(id) { return document.getElementById(id); }
function esc(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// ── Boot ──────────────────────────────────────────────────────
async function boot() {
  await loadCerts();
}

async function loadCerts() {
  el('emptyState').style.display  = 'block';
  el('emptyState').innerHTML      = '<span class="spinner"></span> Connecting to all hosts...';
  el('sslContent').style.display  = 'none';

  try {
    var res  = await fetch('/api/ssl');
    var data = await res.json();
    if (!data.success) throw new Error(data.error || 'Failed to check certificates');
    _data = data;
    render(data);
  } catch(e) {
    el('emptyState').innerHTML = '<div class="error-box">&#x2717; ' + esc(e.message) + '</div>';
  }
}

// ── Render ────────────────────────────────────────────────────
function render(data) {
  el('emptyState').style.display = 'none';
  el('sslContent').style.display = 'block';

  var results = data.results || [];
  var s       = data.summary || {};

  // ── Stat cards ────────────────────────────────────────────
  var statsHtml =
    '<div class="stats" style="margin-bottom:24px">' +
    statCard(results.length,   'Hosts Checked', 'var(--accent)') +
    statCard(s.ok       || 0,  'Valid',          'var(--success)') +
    statCard(s.warning  || 0,  'Expiring Soon',  s.warning  ? 'var(--warn)'    : 'var(--success)') +
    statCard(s.critical || 0,  'Critical',       s.critical ? 'var(--danger)'  : 'var(--success)') +
    statCard(s.expired  || 0,  'Expired',        s.expired  ? 'var(--danger)'  : 'var(--success)') +
    statCard(s.error    || 0,  'Unreachable',    s.error    ? 'var(--muted)'   : 'var(--success)') +
    '</div>';

  // ── Table ─────────────────────────────────────────────────
  // Sort: expired → critical → warning → error → ok
  var order = { expired: 0, critical: 1, warning: 2, error: 3, unknown: 4, ok: 5 };
  results = results.slice().sort(function(a, b) {
    return (order[a.status] || 99) - (order[b.status] || 99);
  });

  var rows = results.map(function(r) {
    var statusCell = statusBadge(r.status, r.days_left);

    var daysCell = r.days_left !== null && r.days_left !== undefined
      ? '<span class="days-' + r.status + '">' + (r.days_left < 0 ? 'Expired ' + Math.abs(r.days_left) + 'd ago' : r.days_left + ' days') + '</span>'
      : '<span class="na">\u2014</span>';

    var expiryCell = r.expires
      ? '<span class="mono-val">' + esc(formatDate(r.expires)) + '</span>'
      : '<span class="na">\u2014</span>';

    var cnCell = r.cn
      ? '<span class="mono-val">' + esc(r.cn) + '</span>'
      : (r.error ? '<span class="error-text">' + esc(r.error) + '</span>' : '<span class="na">\u2014</span>');

    var sansCell = r.sans && r.sans.length
      ? r.sans.slice(0, 3).map(function(s){ return '<span class="group-tag">' + esc(s) + '</span>'; }).join(' ')
        + (r.sans.length > 3 ? ' <span class="na">+' + (r.sans.length - 3) + ' more</span>' : '')
      : '<span class="na">\u2014</span>';

    var issuerCell = r.issuer_cn
      ? '<span class="mono-val">' + esc(r.issuer_cn) + '</span>'
        + (r.issuer_org ? '<br><span class="na">' + esc(r.issuer_org) + '</span>' : '')
      : '<span class="na">\u2014</span>';

    return '<tr class="row-' + r.status + '">'
      + '<td><span class="env-label-badge">' + esc(r.env_label) + '</span></td>'
      + '<td><strong>' + esc(r.host_label) + '</strong><br><span class="na">' + esc(r.hostname) + '</span></td>'
      + '<td>' + statusCell + '</td>'
      + '<td>' + daysCell + '</td>'
      + '<td>' + expiryCell + '</td>'
      + '<td>' + cnCell + '</td>'
      + '<td>' + sansCell + '</td>'
      + '<td>' + issuerCell + '</td>'
      + '<td>' + (r.serial ? '<span class="mono-val serial-val">' + esc(String(r.serial)) + '</span>' : '<span class="na">—</span>') + '</td>'
      + '</tr>';
  }).join('');

  var tableHtml =
    '<div class="section-meta">'
    + '<h2>Certificate Status</h2>'
    + '<span class="count-badge">' + results.length + ' hosts</span>'
    + '<span class="endpoint-badge">TLS handshake :443</span>'
    + '</div>'
    + '<div class="search-wrap"><input class="search" id="sslFilter" placeholder="Filter by host, env, CN or issuer..."></div>'
    + '<div class="table-wrap"><table id="sslTable">'
    + '<thead><tr><th>Env</th><th>Host</th><th>Status</th><th>Days Left</th><th>Expires</th><th>CN</th><th>SANs</th><th>Issuer</th><th>Serial</th></tr></thead>'
    + '<tbody>' + rows + '</tbody>'
    + '</table></div>';

  el('sslContent').innerHTML = statsHtml + tableHtml;

  var f = el('sslFilter');
  if (f) f.oninput = function() { filterTable(this.value); };
}

// ── Helpers ───────────────────────────────────────────────────
function statusBadge(status, days) {
  var labels = { ok: '&#x2713; Valid', warning: '&#x26A0; Expiring', critical: '&#x26A0; Critical', expired: '&#x2717; Expired', error: '&#x2717; Error', unknown: '? Unknown' };
  var label  = labels[status] || status;
  return '<span class="status-chip ssl-' + status + '">' + label + '</span>';
}

function formatDate(certDate) {
  // certDate from ssl is like "Jan 15 12:00:00 2026 GMT"
  try {
    return new Date(certDate).toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' });
  } catch(e) { return certDate; }
}

function statCard(val, label, color) {
  return '<div class="stat"><div class="stat-val" style="color:' + color + '">' + val + '</div><div class="stat-label">' + label + '</div></div>';
}

function filterTable(q) {
  q = q.toLowerCase();
  document.querySelectorAll('#sslTable tbody tr').forEach(function(row) {
    row.style.display = row.textContent.toLowerCase().includes(q) ? '' : 'none';
  });
}

// ── CSV export ────────────────────────────────────────────────
function exportCSV() {
  if (!_data) return;
  var rows = [['Env','Host','Hostname','Status','Days Left','Expires','Issued','CN','SANs','Issuer CN','Issuer Org','Serial']];
  (_data.results || []).forEach(function(r) {
    rows.push([
      r.env_label, r.host_label, r.hostname, r.status,
      r.days_left !== undefined ? r.days_left : '',
      r.expires || '', r.issued || '',
      r.cn || '', (r.sans || []).join(' | '),
      r.issuer_cn || '', r.issuer_org || '', r.serial || '',
    ]);
  });
  var csv  = rows.map(function(r){ return r.map(function(c){ return '"'+String(c||'').replace(/"/g,'""')+'"'; }).join(','); }).join('\r\n');
  var blob = new Blob([csv], {type:'text/csv'});
  var url  = URL.createObjectURL(blob);
  var a    = document.createElement('a');
  a.href=url; a.download='ssl-certs.csv'; a.click();
  URL.revokeObjectURL(url);
}

window.addEventListener('DOMContentLoaded', boot);
