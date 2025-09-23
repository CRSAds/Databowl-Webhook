function byISO(a, b) {
  return a.date_iso.localeCompare(b.date_iso);
}

function mapTreeToRows(tree) {
  return (tree || []).map(n => ({
    key: `day-${n.key}`,   // uniek prefix voor dag
    date_nl: n.label,
    date_iso: n.key,
    leads: Number(n.leads || 0),
    cost: Number(n.cost || 0),
    children: (n.children || []).map(a => ({
      key: `aff-${n.key}-${a.key}`,  // combineer dag + affiliate
      label: a.label,
      leads: Number(a.leads||0),
      cost: Number(a.cost||0),
      children: (a.children || []).map(o => ({
        key: `off-${n.key}-${a.key}-${o.key}`, // dag + aff + offer
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
    const tr = el('tr','level1' + (openDays.has(d.key) ? ' open':''));
    const td1 = el('td','ld-control');
    td1.innerHTML = (d.children && d.children.length ? `<span class="ld-arrow">${openDays.has(d.key)?'▼':'▶'}</span>${d.date_nl}` : d.date_nl);
    tr.appendChild(td1);
    tr.appendChild(el('td','', ''));
    tr.appendChild(el('td','', ''));
    tr.appendChild(el('td','', String(d.leads)));
    tr.appendChild(el('td','', fmtMoney(d.cost)));
    tr.dataset.day = d.key; // nu uniek door prefix
    tbody.appendChild(tr);

    if (openDays.has(d.key)) {
      const openAff = openAffByDay.get(d.key) || new Set();
      for (const a of d.children) {
        const tr2 = el('tr','level2');
        tr2.appendChild(el('td','', ''));
        const tdAff = el('td','ld-control');
        tdAff.innerHTML = (a.children && a.children.length ? `<span class="ld-arrow">${openAff.has(a.key)?'▼':'▶'}</span>${a.label}` : a.label);
        tr2.appendChild(tdAff);
        tr2.appendChild(el('td','', ''));
        tr2.appendChild(el('td','', String(a.leads)));
        tr2.appendChild(el('td','', fmtMoney(a.cost)));
        tr2.dataset.day = d.key;
        tr2.dataset.aff = a.key; // nu uniek door combinatie
        tbody.appendChild(tr2);

        if (openAff.has(a.key) && a.children && a.children.length) {
          for (const o of a.children) {
            const tr3 = el('tr','level3');
            tr3.appendChild(el('td','', ''));
            tr3.appendChild(el('td','', ''));
            tr3.appendChild(el('td','', o.label));
            tr3.appendChild(el('td','', String(o.leads)));
            tr3.appendChild(el('td','', fmtMoney(o.cost)));
            tr3.dataset.day = d.key;
            tr3.dataset.aff = a.key;
            tbody.appendChild(tr3);
          }
        }
      }
    }
  }
}
