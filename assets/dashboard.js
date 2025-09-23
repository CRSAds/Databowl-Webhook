/* globals window, document, fetch, Intl */
(() => {
  const BASE = 'https://databowl-webhook.vercel.app'; // jouw Vercel base
  const root = document.getElementById('lead-dashboard');

  // ---- helpers ----
  const fmtMoney = (n) => Number(n || 0).toFixed(2);
  const setHTML = (el, html) => { el.innerHTML = html; return el; };
  const el = (tag, cls, text) => {
    const e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text != null) e.textContent = text;
    return e;
  };
  const byId = (id) => document.getElementById(id);

  function firstDayOfThisMonthISO() {
    const t = new Date();
    const d = new Date(t.getFullYear(), t.getMonth(), 1);
    return d.toISOString().slice(0,10);
  }
  function todayISO() {
    const t = new Date();
    return t.toISOString().slice(0,10);
  }

  // ---- UI skeleton ----
  const toolbar = el('div', 'ld-toolbar');
  toolbar.innerHTML = `
    <label>Offer ID
      <select id="ld-offer"><option value="">Alle</option></select>
    </label>
    <label>Campaign ID
      <select id="ld-campaign"><option value="">Alle</option></select>
    </label>
    <label>Affiliate ID
      <select id="ld-affiliate"><option value="">Alle</option></select>
    </label>
    <label>Sub ID
      <select id="ld-sub"><option value="">Alle</option></select>
    </label>
    <label>Vanaf
      <input type="date" id="ld-from" />
    </label>
    <label>Tot
      <input type="date" id="ld-to" />
    </label>
    <button class="ld-btn" id="ld-apply">Toepassen</button>
  `;

  const statusBar = el('div', 'ld-status', '');
  const tableWrap = el('div', 'ld-table-wrap');
  const table = el('table', 'ld');
  const thead = el('thead');
  const tbody = el('tbody');
  const totals = el('div', 'ld-totals');

  thead.innerHTML = `
    <tr>
      <th>Datum</th>
      <th>Affiliate ID</th>
      <th>Offer ID</th>
      <th>Aantal Leads</th>
      <th>Kosten</th>
    </tr>
  `;
  table.appendChild(thead);
  table.appendChild(tbody);
  tableWrap.appendChild(table);

  root.appendChild(toolbar);
  root.appendChild(statusBar);
  root.appendChild(tableWrap);
  root.appendChild(totals);

  // ---- state ----
  const openDays = new Set();
  const openAffByDay = new Map();
  let dayRows = [];

  // ---- filters ----
  async function fetchFilters() {
    const r = await fetch(`${BASE}/api/dashboard-filters`);
    if (!r.ok) throw new Error('Kon filters niet laden');
    const { data } = await r.json();
    fillSelect('ld-offer', data.offer_ids);
    fillSelect('ld-campaign', data.campaign_ids);
    fillSelect('ld-affiliate', data.affiliate_ids);
    fillSelect('ld-sub', data.sub_ids || []);
  }
  function fillSelect(id, arr) {
    const sel = byId(id);
    (arr || []).forEach(v => {
      const o = document.createElement('option');
      o.value = v; o.textContent = v;
      sel.appendChild(o);
    });
  }

  // ---- data ----
  async function fetchTree() {
    const params = new URLSearchParams();
    const v = id => byId(id).value;
    if (v('ld-offer'))     params.append('offer_id', v('ld-offer'));
    if (v('ld-campaign'))  params.append('campaign_id', v('ld-campaign'));
    if (v('ld-affiliate')) params.append('affiliate_id', v('ld-affiliate'));
    if (v('ld-sub'))       params.append('sub_id', v('ld-sub'));
    if (v('ld-from'))      params.append('date_from', v('ld-from'));
    if (v('ld-to'))        params.append('date_to', v('ld-to'));

    const r = await fetch(`${BASE}/api/dashboard-aggregate?${params.toString()}`);
    if (!r.ok) {
      const txt = await r.text().catch(()=> '');
      throw new Error(txt || 'Kon dashboard data niet laden');
    }
    const { data } = await r.json();
    return data.tree || [];
  }

  // ---- rendering ----
  function byISO(a,b){ return a.date_iso.localeCompare(b.date_iso); }

  function mapTreeToRows(tree) {
    return (tree || []).map(n => ({
      key: n.key,
      uniq: `day-${n.key}`,
      date_nl: n.label,
      date_iso: n.key,
      leads: Number(n.leads || 0),
      cost: Number(n.cost || 0),
      children: (n.children || []).map(a => ({
        key: a.key,
        uniq: `aff-${n.key}-${a.key}`,
        label: a.label,
        leads: Number(a.leads||0),
        cost: Number(a.cost||0),
        children: (a.children || []).map(o => ({
          key: o.key,
          uniq: `off-${n.key}-${a.key}-${o.key}`,
          label: o.label,
          leads: Number(o.leads||0),
          cost: Number(o.cost||0),
          campaign_id: o.campaign_id ?? null
        }))
      }))
    })).sort(byISO);
  }

  function render() {
    tbody.innerHTML = '';
    if (!dayRows.length) {
      const tr = el('tr','');
      const td = el('td','ld-empty','Geen data voor deze selectie.');
      td.colSpan = 5; tr.appendChild(td); tbody.appendChild(tr);
      totals.textContent = '';
      return;
    }

    const totalLeads = dayRows.reduce((s,r)=> s + (r.leads||0), 0);
    const totalCost  = dayRows.reduce((s,r)=> s + (r.cost ||0), 0);
    totals.textContent = `Totaal: ${totalLeads.toLocaleString('nl-NL')} leads • Kosten € ${fmtMoney(totalCost)}`;

    for (const d of dayRows) {
      const tr = el('tr','level1' + (openDays.has(d.uniq) ? ' open':''));
      const td1 = el('td','ld-control');
      td1.innerHTML = (d.children && d.children.length ? `<span class="ld-arrow">${openDays.has(d.uniq)?'▼':'▶'}</span>${d.date_nl}` : d.date_nl);
      tr.appendChild(td1);
      tr.appendChild(el('td','', ''));
      tr.appendChild(el('td','', ''));
      tr.appendChild(el('td','', String(d.leads)));
      tr.appendChild(el('td','', fmtMoney(d.cost)));
      tr.dataset.day = d.uniq;
      tbody.appendChild(tr);

      if (openDays.has(d.uniq)) {
        const openAff = openAffByDay.get(d.uniq) || new Set();
        for (const a of d.children) {
          const tr2 = el('tr','level2');
          tr2.appendChild(el('td','', ''));
          const tdAff = el('td','ld-control');
          tdAff.innerHTML = (a.children && a.children.length ? `<span class="ld-arrow">${openAff.has(a.uniq)?'▼':'▶'}</span>${a.label}` : a.label);
          tr2.appendChild(tdAff);
          tr2.appendChild(el('td','', ''));
          tr2.appendChild(el('td','', String(a.leads)));
          tr2.appendChild(el('td','', fmtMoney(a.cost)));
          tr2.dataset.day = d.uniq;
          tr2.dataset.aff = a.uniq;
          tbody.appendChild(tr2);

          if (openAff.has(a.uniq) && a.children && a.children.length) {
            for (const o of a.children) {
              const tr3 = el('tr','level3');
              tr3.appendChild(el('td','', ''));
              tr3.appendChild(el('td','', ''));
              tr3.appendChild(el('td','', o.label));
              tr3.appendChild(el('td','', String(o.leads)));
              tr3.appendChild(el('td','', fmtMoney(o.cost)));
              tr3.dataset.day = d.uniq;
              tr3.dataset.aff = a.uniq;
              tbody.appendChild(tr3);
            }
          }
        }
      }
    }
  }

  // ---- interactions ----
  tbody.addEventListener('click', (ev) => {
    const td = ev.target.closest('td');
    if (!td || !td.classList.contains('ld-control')) return;
    const tr = td.parentElement;

    if (tr.classList.contains('level1')) {
      const dayKey = tr.dataset.day;
      if (openDays.has(dayKey)) openDays.delete(dayKey);
      else openDays.add(dayKey);
      render();
      return;
    }

    if (tr.classList.contains('level2')) {
      const dayKey = tr.dataset.day;
      const affKey = tr.dataset.aff;
      if (!openAffByDay.has(dayKey)) openAffByDay.set(dayKey, new Set());
      const set = openAffByDay.get(dayKey);
      if (set.has(affKey)) set.delete(affKey); else set.add(affKey);
      render();
      return;
    }
  });

  // ---- load / apply ----
  async function apply() {
    try {
      const now = new Date();
      setHTML(statusBar, `Bijgewerkt: ${now.toLocaleTimeString('nl-NL')}`);
      const tree = await fetchTree();
      dayRows = mapTreeToRows(tree);
      render();
    } catch (e) {
      setHTML(statusBar, '');
      alert(e?.message || 'Kon data niet laden.');
    }
  }

  // defaults
  (function defaults() {
    byId('ld-from').value = firstDayOfThisMonthISO();
    byId('ld-to').value = todayISO();
  })();

  byId('ld-apply').addEventListener('click', apply);

  // init
  (async () => {
    await fetchFilters();
    await apply();
  })();
})();
