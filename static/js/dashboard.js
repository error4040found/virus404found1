/* ================================================================
   dashboard.js  –  Pinpointe Campaign Dashboard (FastAPI frontend)
   ================================================================ */

let currentView   = 'today';   // 'today' | 'range'
let currentReport = 'campaigns'; // 'campaigns' | 'seeds'
let isLoading     = false;
let isSyncing     = false;

// ─── Domain pagination & search state ────────────────────────────
let allDomains      = [];
let domainPage      = 1;
let domainsPerPage  = 3;
let domainSearchTerm = '';

// ─── Bootstrap ───────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
    initDates();
    bindEvents();
    loadCampaigns();
});

function initDates() {
    const today     = new Date();
    const yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);
    const fmt = d => d.toISOString().slice(0, 10);
    document.getElementById('start-date').value = fmt(yesterday);
    document.getElementById('end-date').value   = fmt(today);
}

// ─── Event wiring ────────────────────────────────────────────────
function bindEvents() {
    // Report tabs (Campaigns / Seeds)
    document.querySelectorAll('.tab').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.tab').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            currentReport = btn.dataset.report;
            loadCampaigns();
        });
    });

    // View buttons (Today / Range)
    document.querySelectorAll('.view-btn').forEach(btn => {
        btn.addEventListener('click', async () => {
            document.querySelectorAll('.view-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            currentView = btn.dataset.view;
            document.getElementById('date-range').style.display =
                currentView === 'range' ? 'flex' : 'none';
            loadCampaigns();
        });
    });

    // Date change — just reload from DB, user must click Sync to fetch fresh
    document.getElementById('start-date').addEventListener('change', () => loadCampaigns());
    document.getElementById('end-date').addEventListener('change', () => loadCampaigns());

    // Domain search with debounce
    let searchTimeout;
    document.getElementById('domain-search').addEventListener('input', (e) => {
        clearTimeout(searchTimeout);
        searchTimeout = setTimeout(() => {
            domainSearchTerm = e.target.value.trim().toLowerCase();
            domainPage = 1;
            renderDomainPage();
        }, 250);
    });

    // Sync button
    document.getElementById('sync-btn').addEventListener('click', syncCampaigns);
}

// ─── Load campaigns from DB ──────────────────────────────────────
async function loadCampaigns() {
    if (isLoading) return;
    try {
        isLoading = true;
        showLoading(true);
        hideError();

        let url;
        if (currentReport === 'seeds') {
            url = currentView === 'today'
                ? '/api/seeds/today'
                : `/api/seeds/range?startDate=${val('start-date')}&endDate=${val('end-date')}`;
        } else {
            url = currentView === 'today'
                ? '/api/today'
                : `/api/range?startDate=${val('start-date')}&endDate=${val('end-date')}`;
        }

        const res  = await fetch(url);
        const data = await res.json();
        if (!data.success) throw new Error(data.error || 'Load failed');

        // Auto-sync if today view returns no data (first visit / new day)
        if ((!data.domains || !data.domains.length) && currentView === 'today' && !isSyncing) {
            isLoading = false;
            showLoading(false);
            await syncCampaigns();
            return;
        }

        displayCampaigns(data.domains);
        updateLastSync();
    } catch (err) {
        showError(err.message);
    } finally {
        isLoading = false;
        showLoading(false);
    }
}

// ─── Sync from Pinpointe API ────────────────────────────────────
async function syncCampaigns() {
    if (isSyncing) return;
    try {
        isSyncing = true;
        setSyncing(true);
        hideError();

        let url;
        if (currentView === 'today') {
            url = '/api/sync/today';
        } else {
            url = `/api/sync/range?startDate=${val('start-date')}&endDate=${val('end-date')}`;
        }

        const res  = await fetch(url, { method: 'POST' });
        const data = await res.json();

        if (data.errors && data.errors.length) {
            showError(data.errors.map(e => `${e.domain||''}: ${e.error||''}`).join(' | '));
        }
        if (!data.success && (!data.errors || !data.errors.length)) {
            throw new Error(data.error || 'Sync failed');
        }

        await loadCampaigns();
    } catch (err) {
        showError(err.message);
    } finally {
        isSyncing = false;
        setSyncing(false);
    }
}

// ─── Render domains + campaigns ──────────────────────────────────
function displayCampaigns(domains) {
    const container = document.getElementById('campaigns-container');
    const noData    = document.getElementById('no-data');

    if (!domains || !domains.length) {
        container.innerHTML = '';
        noData.style.display = 'flex';
        document.getElementById('grand-totals').style.display = 'none';
        document.getElementById('domain-pagination').style.display = 'none';
        return;
    }
    noData.style.display = 'none';

    // Grand totals always reflect ALL domains (unfiltered)
    renderGrandTotals(domains);

    // Store all domains, reset page, render current page
    allDomains = domains;
    domainPage = 1;
    // Keep search term if user was searching (don't clear on reload)
    renderDomainPage();
}

// ─── Filter + paginate + render current domain page ──────────────
function renderDomainPage() {
    const container  = document.getElementById('campaigns-container');
    const noData     = document.getElementById('no-data');

    // Filter by search term
    let filtered = allDomains;
    if (domainSearchTerm) {
        filtered = allDomains.filter(d =>
            d.name.toLowerCase().includes(domainSearchTerm)
        );
    }

    if (!filtered.length) {
        container.innerHTML = `<div class="state-msg"><span class="empty-icon">🔍</span> No domains match "${esc(domainSearchTerm)}"</div>`;
        document.getElementById('domain-pagination').style.display = 'none';
        return;
    }

    // Pagination
    const totalPages = Math.ceil(filtered.length / domainsPerPage);
    if (domainPage > totalPages) domainPage = totalPages;
    const startIdx = (domainPage - 1) * domainsPerPage;
    const pageItems = filtered.slice(startIdx, startIdx + domainsPerPage);

    container.innerHTML = pageItems.map(d => `
        <div class="domain-card">
            <div class="domain-header" onclick="this.parentElement.classList.toggle('collapsed')">
                <div class="domain-title">
                    <span class="domain-indicator">▼</span>
                    <span class="domain-name">${esc(d.name)}</span>
                    <span class="domain-count">${d.campaigns.length} campaign${d.campaigns.length!==1?'s':''}</span>
                </div>
                <div class="domain-stats">
                    <span class="domain-stat"><strong>SENDS</strong> ${fmt(d.totals.sends)}</span>
                    <span class="domain-stat" style="color:${pctColor(d.totals.open_percent,'open')}"><strong>OPEN</strong> ${d.totals.open_percent}%</span>
                    <span class="domain-stat" style="color:${pctColor(d.totals.click_percent,'click')}"><strong>CLICK</strong> ${d.totals.click_percent}%</span>
                    <span class="domain-stat"><strong>BOUNCE</strong> ${fmt(d.totals.bounces)}</span>
                    <span class="domain-stat"><strong>UNSUB</strong> ${fmt(d.totals.unsubs)}</span>
                    <span class="domain-stat visitors-stat"><strong>VISITORS</strong> ${fmt(d.totals.visitors)}</span>
                    <span class="domain-stat leads-stat"><strong>T.LEADS</strong> ${fmt(d.totals.total_leads)}</span>
                    <span class="domain-stat conv-stat"><strong>S.LEADS</strong> ${fmt(d.totals.conversions)}</span>
                    <span class="domain-stat revenue-stat"><strong>REV</strong> $${money(d.totals.revenue)}</span>
                    <span class="domain-stat epc-stat"><strong>EPC</strong> $${money(d.totals.epc)}</span>
                    <span class="domain-stat ecpm-stat"><strong>eCPM</strong> $${money(d.totals.ecpm)}</span>
                </div>
            </div>
            <div class="campaign-table-wrap">
                <table class="campaign-table">
                    <thead>
                        <tr>
                            <th class="col-date">Date / Time</th>
                            <th class="col-name">Campaign</th>
                            <th class="col-num">Sends</th>
                            <th class="col-num">Opens</th>
                            <th class="col-pct">Open %</th>
                            <th class="col-num">Clicks</th>
                            <th class="col-pct">Click %</th>
                            <th class="col-num">Bounces</th>
                            <th class="col-num">Unsubs</th>
                            <th class="col-num-sm">Visitors</th>
                            <th class="col-num-sm">T.Leads</th>
                            <th class="col-num-sm">S.Leads</th>
                            <th class="col-money">Revenue</th>
                            <th class="col-money">EPC</th>
                            <th class="col-money">eCPM</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${d.campaigns.map(c => `
                        <tr>
                            <td class="col-date">${c.date} ${c.time}</td>
                            <td class="col-name" title="${esc(c.campaign_name)}">${esc(c.campaign_name)}</td>
                            <td class="col-num">${fmt(c.sends)}</td>
                            <td class="col-num">${fmt(c.opens)}</td>
                            <td class="col-pct" style="color:${pctColor(c.open_percent,'open')}">${c.open_percent}%</td>
                            <td class="col-num">${fmt(c.clicks)}</td>
                            <td class="col-pct" style="color:${pctColor(c.click_percent,'click')}">${c.click_percent}%</td>
                            <td class="col-num">${fmt(c.bounces)}</td>
                            <td class="col-num">${fmt(c.unsubs)}</td>
                            <td class="col-num-sm visitors-val">${c.visitors > 0 ? fmt(c.visitors) : '-'}</td>
                            <td class="col-num-sm leads-val">${c.total_leads > 0 ? fmt(c.total_leads) : '-'}</td>
                            <td class="col-num-sm conv-val">${c.conversions > 0 ? fmt(c.conversions) : '-'}</td>
                            <td class="col-money revenue-val">${c.revenue > 0 ? '$'+money(c.revenue) : '-'}</td>
                            <td class="col-money epc-val">${c.epc > 0 ? '$'+money(c.epc) : '-'}</td>
                            <td class="col-money ecpm-val">${c.ecpm > 0 ? '$'+money(c.ecpm) : '-'}</td>
                        </tr>`).join('')}
                    </tbody>
                </table>
            </div>
        </div>
    `).join('');

    // Render pagination controls
    renderPagination(filtered.length, totalPages);
}

// ─── Pagination controls ─────────────────────────────────────────
function renderPagination(totalItems, totalPages) {
    const pag = document.getElementById('domain-pagination');
    if (totalPages <= 1) {
        pag.style.display = 'none';
        return;
    }

    let html = `<button class="page-btn" onclick="goToPage(${domainPage - 1})" ${domainPage === 1 ? 'disabled' : ''}>&laquo; Prev</button>`;

    for (let i = 1; i <= totalPages; i++) {
        html += `<button class="page-btn${i === domainPage ? ' active' : ''}" onclick="goToPage(${i})">${i}</button>`;
    }

    html += `<button class="page-btn" onclick="goToPage(${domainPage + 1})" ${domainPage === totalPages ? 'disabled' : ''}>Next &raquo;</button>`;
    html += `<span class="page-info">${totalItems} domain${totalItems !== 1 ? 's' : ''}</span>`;

    pag.innerHTML = html;
    pag.style.display = 'flex';
}

function goToPage(page) {
    const filtered = domainSearchTerm
        ? allDomains.filter(d => d.name.toLowerCase().includes(domainSearchTerm))
        : allDomains;
    const totalPages = Math.ceil(filtered.length / domainsPerPage);
    if (page < 1 || page > totalPages) return;
    domainPage = page;
    renderDomainPage();
    // Scroll to top of campaign cards
    document.getElementById('campaigns-container').scrollIntoView({ behavior: 'smooth', block: 'start' });
}

// ─── Grand totals bar ────────────────────────────────────────────
function renderGrandTotals(domains) {
    const t = domains.reduce((a, d) => {
        a.sends   += d.totals.sends;
        a.opens   += d.totals.opens;
        a.clicks  += d.totals.clicks;
        a.bounces += d.totals.bounces;
        a.unsubs  += d.totals.unsubs;
        a.revenue += d.totals.revenue || 0;
        a.conversions += d.totals.conversions || 0;
        a.visitors += d.totals.visitors || 0;
        a.total_leads += d.totals.total_leads || 0;
        return a;
    }, { sends:0, opens:0, clicks:0, bounces:0, unsubs:0, revenue:0, conversions:0, visitors:0, total_leads:0 });

    const oPct = t.sends > 0 ? ((t.opens / t.sends) * 100).toFixed(2) : '0.00';
    const cPct = t.sends > 0 ? ((t.clicks/ t.sends) * 100).toFixed(2) : '0.00';
    const epc  = t.clicks > 0 ? (t.revenue / t.clicks).toFixed(2) : '0.00';
    const ecpm = t.sends > 0 ? ((t.revenue / t.sends) * 1000).toFixed(2) : '0.00';

    el('total-sends').textContent      = fmt(t.sends);
    el('total-opens').textContent      = `${fmt(t.opens)} opens`;
    el('total-clicks').textContent     = `${fmt(t.clicks)} clicks`;
    el('total-bounces').textContent    = fmt(t.bounces);
    el('total-unsubs').textContent     = fmt(t.unsubs);

    const oEl = el('total-open-pct');
    oEl.textContent = `${oPct}%`;
    oEl.style.color = pctColor(oPct, 'open');

    const cEl = el('total-click-pct');
    cEl.textContent = `${cPct}%`;
    cEl.style.color = pctColor(cPct, 'click');

    // Leadpier totals
    el('total-visitors').textContent    = fmt(t.visitors);
    el('total-total-leads').textContent = fmt(t.total_leads);
    el('total-sold-leads').textContent  = fmt(t.conversions);
    el('total-revenue').textContent     = `$${money(t.revenue)}`;
    el('total-epc').textContent        = `$${epc}`;
    el('total-ecpm').textContent       = `$${ecpm}`;

    el('grand-totals').style.display = 'grid';
}

// ─── Helpers ─────────────────────────────────────────────────────
function el(id)  { return document.getElementById(id); }
function val(id) { return document.getElementById(id).value; }
function fmt(n)  { return parseInt(n || 0).toLocaleString(); }
function money(n){ return parseFloat(n || 0).toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}); }

function esc(text) {
    const d = document.createElement('div');
    d.textContent = text;
    return d.innerHTML;
}

function pctColor(v, type) {
    const n = parseFloat(v);
    if (type === 'open')  return n >= 50 ? '#22c55e' : n >= 30 ? '#eab308' : '#ef4444';
    if (type === 'click') return n >= 1.5 ? '#22c55e' : n >= 1.0 ? '#eab308' : '#ef4444';
    return '#94a3b8';
}

function showLoading(on) {
    el('loading').style.display            = on ? 'flex' : 'none';
    el('campaigns-container').style.display = on ? 'none' : 'flex';
    if (on) el('grand-totals').style.display = 'none';
}

function showError(msg) {
    const e = el('error-message');
    e.textContent = '⚠️ ' + msg;
    e.style.display = 'flex';
}
function hideError() { el('error-message').style.display = 'none'; }

function setSyncing(on) {
    const btn = el('sync-btn');
    if (on) {
        btn.classList.add('syncing');
        btn.disabled = true;
        btn.querySelector('.btn-label').textContent = 'Syncing…';
    } else {
        btn.classList.remove('syncing');
        btn.disabled = false;
        btn.querySelector('.btn-label').textContent = 'Fetch New Stats';
    }
}

function updateLastSync() {
    const now = new Date();
    el('last-sync').textContent = now.toLocaleString('en-US', {
        month:'short', day:'2-digit', year:'numeric',
        hour:'2-digit', minute:'2-digit', second:'2-digit', hour12:false
    });
}
