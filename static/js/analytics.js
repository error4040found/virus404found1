/* ================================================================
   analytics.js  –  Visual Analytics Page (Chart.js)
   Production-ready charts for campaign & Leadpier data
   ================================================================ */

// ─── State ───────────────────────────────────────────────────────
let chartInstances = {};
let analyticsData  = null;
let isSyncing      = false;

// ─── Chart.js global defaults ────────────────────────────────────
Chart.defaults.color        = '#94a3b8';
Chart.defaults.borderColor  = 'rgba(255,255,255,0.06)';
Chart.defaults.font.family  = "'Inter', sans-serif";
Chart.defaults.font.size    = 12;
Chart.defaults.plugins.legend.labels.usePointStyle = true;
Chart.defaults.plugins.legend.labels.pointStyleWidth = 10;
Chart.defaults.plugins.tooltip.backgroundColor = '#1e293b';
Chart.defaults.plugins.tooltip.titleColor      = '#e2e8f0';
Chart.defaults.plugins.tooltip.bodyColor       = '#94a3b8';
Chart.defaults.plugins.tooltip.borderColor     = 'rgba(255,255,255,0.1)';
Chart.defaults.plugins.tooltip.borderWidth     = 1;
Chart.defaults.plugins.tooltip.cornerRadius    = 8;
Chart.defaults.plugins.tooltip.padding         = 10;

// Color palette
const C = {
    teal:     '#14b8a6',
    tealBg:   'rgba(20,184,166,0.25)',
    blue:     '#3b82f6',
    blueBg:   'rgba(59,130,246,0.25)',
    purple:   '#8b5cf6',
    purpleBg: 'rgba(139,92,246,0.25)',
    cyan:     '#06b6d4',
    cyanBg:   'rgba(6,182,212,0.25)',
    green:    '#22c55e',
    greenBg:  'rgba(34,197,94,0.25)',
    yellow:   '#eab308',
    yellowBg: 'rgba(234,179,8,0.25)',
    red:      '#ef4444',
    redBg:    'rgba(239,68,68,0.25)',
    orange:   '#f97316',
    orangeBg: 'rgba(249,115,22,0.25)',
    pink:     '#ec4899',
    pinkBg:   'rgba(236,72,153,0.25)',
    indigo:   '#6366f1',
    indigoBg: 'rgba(99,102,241,0.25)',
};

// Domain-level colours (cycle through)
const DOMAIN_COLORS = [
    C.teal, C.blue, C.purple, C.cyan, C.green, C.yellow, C.orange, C.pink, C.indigo, C.red,
];
const DOMAIN_BG = [
    C.tealBg, C.blueBg, C.purpleBg, C.cyanBg, C.greenBg, C.yellowBg, C.orangeBg, C.pinkBg, C.indigoBg, C.redBg,
];

// ─── Bootstrap ───────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
    initDates(7);
    bindEvents();
    loadAnalytics();
});

function initDates(daysBack) {
    const today = new Date();
    const start = new Date(today);
    start.setDate(start.getDate() - daysBack + 1);
    document.getElementById('start-date').value = fmtDate(start);
    document.getElementById('end-date').value   = fmtDate(today);
}

function fmtDate(d) { return d.toISOString().slice(0, 10); }
function fmtShort(dateStr) {
    const d = new Date(dateStr + 'T00:00:00');
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}

// ─── Event Binding ───────────────────────────────────────────────
function bindEvents() {
    document.getElementById('load-btn').addEventListener('click', loadAnalytics);
    document.getElementById('sync-btn').addEventListener('click', syncAndLoad);

    document.querySelectorAll('.range-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.range-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            initDates(parseInt(btn.dataset.range));
            loadAnalytics();
        });
    });

    // Enter key on date inputs
    ['start-date', 'end-date'].forEach(id => {
        document.getElementById(id).addEventListener('keydown', e => {
            if (e.key === 'Enter') loadAnalytics();
        });
    });
}

// ─── Load Analytics Data ─────────────────────────────────────────
async function loadAnalytics() {
    const startDate = document.getElementById('start-date').value;
    const endDate   = document.getElementById('end-date').value;
    if (!startDate || !endDate) return;

    showLoading(true);
    hideError();

    try {
        const res  = await fetch(`/api/analytics?startDate=${startDate}&endDate=${endDate}`);
        const data = await res.json();
        if (!data.success) throw new Error(data.error || 'Failed to load analytics');

        // If no data, auto-sync first
        if ((!data.daily || !data.daily.length) && !isSyncing) {
            showLoading(false);
            await syncAndLoad();
            return;
        }

        analyticsData = data;
        renderKPIs(data.totals);
        renderAllCharts(data);
        document.getElementById('kpi-bar').style.display      = 'grid';
        document.getElementById('revenue-bar').style.display   = 'grid';
        document.getElementById('charts-section').style.display = 'block';
    } catch (err) {
        showError(err.message);
    } finally {
        showLoading(false);
    }
}

// ─── Sync then reload ────────────────────────────────────────────
async function syncAndLoad() {
    if (isSyncing) return;
    const startDate = document.getElementById('start-date').value;
    const endDate   = document.getElementById('end-date').value;

    try {
        isSyncing = true;
        setSyncing(true);
        hideError();

        const res  = await fetch(`/api/sync/range?startDate=${startDate}&endDate=${endDate}`, { method: 'POST' });
        const data = await res.json();

        if (data.errors && data.errors.length) {
            showError(data.errors.map(e => `${e.domain||''}: ${e.error||''}`).join(' | '));
        }

        await loadAnalytics();
    } catch (err) {
        showError(err.message);
    } finally {
        isSyncing = false;
        setSyncing(false);
    }
}

// ─── Render KPIs ─────────────────────────────────────────────────
function renderKPIs(t) {
    if (!t) return;
    el('kpi-sends').textContent     = fmt(t.sends);
    el('kpi-opens').textContent     = fmt(t.opens);
    el('kpi-open-rate').textContent = t.open_pct.toFixed(2) + '%';
    el('kpi-ctr').textContent       = t.click_pct.toFixed(2) + '%';
    el('kpi-clicks').textContent    = fmt(t.clicks);
    el('kpi-unsubs').textContent    = fmt(t.unsubs);

    // Revenue KPIs
    el('rev-le').textContent       = '$' + money(t.revenue);
    el('rev-le-sales').textContent = fmt(t.conversions) + ' sales';
    el('rev-total').textContent    = '$' + money(t.revenue);
    el('rev-sales').textContent    = fmt(t.conversions);
    el('rev-ecpm').textContent     = '$' + money(t.ecpm);
    el('rev-epc').textContent      = '$' + money(t.epc);
    el('rev-visitors').textContent = fmt(t.visitors);
}

// ─── Master Render ───────────────────────────────────────────────
function renderAllCharts(data) {
    const daily   = data.daily   || [];
    const domains = data.domains || [];
    const labels  = daily.map(d => fmtShort(d.date));

    // Row 1 — Daily charts
    renderSendsChart(labels, daily);
    renderRatesChart(labels, daily);
    renderRevenueChart(labels, daily);

    // Row 2 — Daily charts
    renderOpensClicksChart(labels, daily);
    renderUnsubsBouncesChart(labels, daily);
    renderSalesChart(labels, daily);

    // Row 3 — Trend charts
    renderOpenTrend(labels, daily);
    renderCtrTrend(labels, daily);
    renderEcpmTrend(labels, daily);

    // Row 4-5 — Domain comparison
    renderDomainOpen(domains);
    renderDomainCtr(domains);
    renderDomainEcpm(domains);
    renderDomainRevenue(domains);
    renderDomainSends(domains);
    renderDomainLeads(domains);
}

// ─── Chart Builders ──────────────────────────────────────────────

function makeChart(id, config) {
    if (chartInstances[id]) chartInstances[id].destroy();
    const ctx = document.getElementById(id).getContext('2d');
    chartInstances[id] = new Chart(ctx, config);
    return chartInstances[id];
}

// Grid line style for all charts
const gridStyle = {
    color: 'rgba(255,255,255,0.05)',
    drawBorder: false,
};

// --- Row 1 ---

function renderSendsChart(labels, daily) {
    makeChart('chart-sends', {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                label: 'Sends',
                data: daily.map(d => d.sends),
                backgroundColor: createGradientBar('chart-sends', C.teal, C.tealBg),
                borderColor: C.teal,
                borderWidth: 1,
                borderRadius: 6,
                borderSkipped: false,
            }],
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { grid: gridStyle },
                y: { grid: gridStyle, beginAtZero: true,
                     ticks: { callback: v => v >= 1000 ? (v/1000).toFixed(0)+'k' : v }
                },
            },
            interaction: { mode: 'index', intersect: false },
        },
    });
}

function renderRatesChart(labels, daily) {
    makeChart('chart-rates', {
        type: 'bar',
        data: {
            labels,
            datasets: [
                {
                    label: 'Open %',
                    data: daily.map(d => d.open_pct),
                    backgroundColor: C.blueBg,
                    borderColor: C.blue,
                    borderWidth: 1,
                    borderRadius: 4,
                    borderSkipped: false,
                },
                {
                    label: 'CTR %',
                    data: daily.map(d => d.click_pct),
                    backgroundColor: C.greenBg,
                    borderColor: C.green,
                    borderWidth: 1,
                    borderRadius: 4,
                    borderSkipped: false,
                },
            ],
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { position: 'top' } },
            scales: {
                x: { grid: gridStyle },
                y: { grid: gridStyle, beginAtZero: true,
                     ticks: { callback: v => v + '%' }
                },
            },
            interaction: { mode: 'index', intersect: false },
        },
    });
}

function renderRevenueChart(labels, daily) {
    makeChart('chart-revenue', {
        type: 'bar',
        data: {
            labels,
            datasets: [
                {
                    label: 'LE Revenue',
                    data: daily.map(d => d.revenue),
                    backgroundColor: C.purpleBg,
                    borderColor: C.purple,
                    borderWidth: 1,
                    borderRadius: 6,
                    borderSkipped: false,
                    yAxisID: 'y',
                },
                {
                    label: 'eCPM $',
                    data: daily.map(d => d.ecpm),
                    type: 'line',
                    borderColor: C.cyan,
                    backgroundColor: 'transparent',
                    borderWidth: 2,
                    pointRadius: 3,
                    pointBackgroundColor: C.cyan,
                    tension: 0.3,
                    yAxisID: 'y1',
                },
            ],
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { position: 'top' } },
            scales: {
                x: { grid: gridStyle },
                y: { grid: gridStyle, position: 'left', beginAtZero: true,
                     ticks: { callback: v => '$' + v }
                },
                y1: { grid: { display: false }, position: 'right', beginAtZero: true,
                      ticks: { callback: v => '$' + v.toFixed(2) }
                },
            },
            interaction: { mode: 'index', intersect: false },
        },
    });
}

// --- Row 2 ---

function renderOpensClicksChart(labels, daily) {
    makeChart('chart-opens-clicks', {
        type: 'bar',
        data: {
            labels,
            datasets: [
                {
                    label: 'Opens',
                    data: daily.map(d => d.opens),
                    backgroundColor: C.blueBg,
                    borderColor: C.blue,
                    borderWidth: 1,
                    borderRadius: 4,
                    borderSkipped: false,
                },
                {
                    label: 'Clicks',
                    data: daily.map(d => d.clicks),
                    backgroundColor: C.purpleBg,
                    borderColor: C.purple,
                    borderWidth: 1,
                    borderRadius: 4,
                    borderSkipped: false,
                },
            ],
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { position: 'top' } },
            scales: {
                x: { grid: gridStyle },
                y: { grid: gridStyle, beginAtZero: true,
                     ticks: { callback: v => v >= 1000 ? (v/1000).toFixed(0)+'k' : v }
                },
            },
            interaction: { mode: 'index', intersect: false },
        },
    });
}

function renderUnsubsBouncesChart(labels, daily) {
    makeChart('chart-unsubs-bounces', {
        type: 'bar',
        data: {
            labels,
            datasets: [
                {
                    label: 'Unsubs',
                    data: daily.map(d => d.unsubs),
                    backgroundColor: C.redBg,
                    borderColor: C.red,
                    borderWidth: 1,
                    borderRadius: 4,
                    borderSkipped: false,
                },
                {
                    label: 'Bounces',
                    data: daily.map(d => d.bounces),
                    backgroundColor: C.orangeBg,
                    borderColor: C.orange,
                    borderWidth: 1,
                    borderRadius: 4,
                    borderSkipped: false,
                },
            ],
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { position: 'top' } },
            scales: {
                x: { grid: gridStyle },
                y: { grid: gridStyle, beginAtZero: true },
            },
            interaction: { mode: 'index', intersect: false },
        },
    });
}

function renderSalesChart(labels, daily) {
    makeChart('chart-sales', {
        type: 'bar',
        data: {
            labels,
            datasets: [
                {
                    label: 'LE Sales',
                    data: daily.map(d => d.conversions),
                    backgroundColor: C.purpleBg,
                    borderColor: C.purple,
                    borderWidth: 1,
                    borderRadius: 6,
                    borderSkipped: false,
                },
                {
                    label: 'Visitors',
                    data: daily.map(d => d.visitors),
                    type: 'line',
                    borderColor: C.cyan,
                    backgroundColor: 'transparent',
                    borderWidth: 2,
                    pointRadius: 3,
                    pointBackgroundColor: C.cyan,
                    tension: 0.3,
                },
            ],
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { position: 'top' } },
            scales: {
                x: { grid: gridStyle },
                y: { grid: gridStyle, beginAtZero: true },
            },
            interaction: { mode: 'index', intersect: false },
        },
    });
}

// --- Row 3 — Trend Charts ---

function renderOpenTrend(labels, daily) {
    makeChart('chart-open-trend', {
        type: 'bar',
        data: {
            labels,
            datasets: [
                {
                    label: 'Open%',
                    data: daily.map(d => d.open_pct),
                    backgroundColor: createGradientBar('chart-open-trend', '#c48a2c', '#c48a2c80'),
                    borderColor: '#c48a2c',
                    borderWidth: 1,
                    borderRadius: 4,
                    borderSkipped: false,
                    order: 1,
                },
                {
                    label: 'Trendline',
                    data: computeTrendline(daily.map(d => d.open_pct)),
                    type: 'line',
                    borderColor: '#a78bfa',
                    backgroundColor: 'transparent',
                    borderWidth: 2,
                    borderDash: [6, 3],
                    pointRadius: 0,
                    tension: 0.4,
                    order: 0,
                },
            ],
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { position: 'top' },
                tooltip: {
                    callbacks: {
                        label: ctx => ctx.dataset.label + ': ' + ctx.parsed.y.toFixed(2) + '%'
                    }
                }
            },
            scales: {
                x: { grid: gridStyle },
                y: { grid: gridStyle, beginAtZero: true,
                     ticks: { callback: v => v.toFixed(0) + '%' }
                },
            },
        },
    });
}

function renderCtrTrend(labels, daily) {
    makeChart('chart-ctr-trend', {
        type: 'line',
        data: {
            labels,
            datasets: [
                {
                    label: 'CTR%',
                    data: daily.map(d => d.click_pct),
                    borderColor: C.green,
                    backgroundColor: C.greenBg,
                    borderWidth: 2,
                    pointRadius: 4,
                    pointBackgroundColor: C.green,
                    fill: true,
                    tension: 0.3,
                },
                {
                    label: 'Trendline',
                    data: computeTrendline(daily.map(d => d.click_pct)),
                    borderColor: C.orange,
                    backgroundColor: 'transparent',
                    borderWidth: 2,
                    borderDash: [6, 3],
                    pointRadius: 0,
                    tension: 0.4,
                },
            ],
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { position: 'top' },
                tooltip: {
                    callbacks: {
                        label: ctx => ctx.dataset.label + ': ' + ctx.parsed.y.toFixed(2) + '%'
                    }
                }
            },
            scales: {
                x: { grid: gridStyle },
                y: { grid: gridStyle, beginAtZero: true,
                     ticks: { callback: v => v.toFixed(2) + '%' }
                },
            },
        },
    });
}

function renderEcpmTrend(labels, daily) {
    makeChart('chart-ecpm-trend', {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                label: 'eCPM',
                data: daily.map(d => d.ecpm),
                backgroundColor: createGradientBar('chart-ecpm-trend', C.blue, C.blueBg),
                borderColor: C.blue,
                borderWidth: 1,
                borderRadius: 6,
                borderSkipped: false,
            }],
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false },
                tooltip: {
                    callbacks: {
                        label: ctx => 'eCPM: $' + ctx.parsed.y.toFixed(2)
                    }
                }
            },
            scales: {
                x: { grid: gridStyle },
                y: { grid: gridStyle, beginAtZero: true,
                     ticks: { callback: v => '$' + v.toFixed(2) }
                },
            },
        },
    });
}

// --- Row 4-5 — Domain Comparison Charts ---

function renderDomainOpen(domains) {
    const names = domains.map(d => d.name);
    const vals  = domains.map(d => d.open_pct);
    const grandAvg = domains.length ? (vals.reduce((a,b)=>a+b,0) / vals.length) : 0;
    names.push('Grand Total');
    vals.push(parseFloat(grandAvg.toFixed(2)));

    makeChart('chart-domain-open', {
        type: 'bar',
        data: {
            labels: names,
            datasets: [{
                label: 'Open%',
                data: vals,
                backgroundColor: names.map((_, i) => i < names.length - 1 ? C.greenBg : C.tealBg),
                borderColor: names.map((_, i) => i < names.length - 1 ? C.green : C.teal),
                borderWidth: 1,
                borderRadius: 6,
                borderSkipped: false,
            }],
        },
        options: domainBarOptions('%'),
    });
}

function renderDomainCtr(domains) {
    const names = domains.map(d => d.name);
    const vals  = domains.map(d => d.click_pct);
    const grandAvg = domains.length ? (vals.reduce((a,b)=>a+b,0) / vals.length) : 0;
    names.push('Grand Total');
    vals.push(parseFloat(grandAvg.toFixed(2)));

    makeChart('chart-domain-ctr', {
        type: 'bar',
        data: {
            labels: names,
            datasets: [{
                label: 'CTR%',
                data: vals,
                backgroundColor: names.map((_, i) => i < names.length - 1 ? C.yellowBg : C.tealBg),
                borderColor: names.map((_, i) => i < names.length - 1 ? C.yellow : C.teal),
                borderWidth: 1,
                borderRadius: 6,
                borderSkipped: false,
            }],
        },
        options: domainBarOptions('%'),
    });
}

function renderDomainEcpm(domains) {
    const names = domains.map(d => d.name);
    const vals  = domains.map(d => d.ecpm);
    const total = domains.reduce((a,d)=> a + d.revenue, 0);
    const totalSends = domains.reduce((a,d)=> a + d.sends, 0);
    const grandEcpm = totalSends > 0 ? (total / totalSends) * 1000 : 0;
    names.push('Grand Total');
    vals.push(parseFloat(grandEcpm.toFixed(2)));

    makeChart('chart-domain-ecpm', {
        type: 'bar',
        data: {
            labels: names,
            datasets: [{
                label: 'eCPM',
                data: vals,
                backgroundColor: names.map((_, i) => i < names.length - 1 ? C.blueBg : C.tealBg),
                borderColor: names.map((_, i) => i < names.length - 1 ? C.blue : C.teal),
                borderWidth: 1,
                borderRadius: 6,
                borderSkipped: false,
            }],
        },
        options: domainBarOptions('$'),
    });
}

function renderDomainRevenue(domains) {
    const names = domains.map(d => d.name);
    const vals  = domains.map(d => d.revenue);
    const total = vals.reduce((a,b) => a+b, 0);
    names.push('Grand Total');
    vals.push(parseFloat(total.toFixed(2)));

    makeChart('chart-domain-revenue', {
        type: 'bar',
        data: {
            labels: names,
            datasets: [{
                label: 'Revenue',
                data: vals,
                backgroundColor: names.map((_, i) => i < names.length - 1 ? DOMAIN_BG[i % DOMAIN_BG.length] : C.tealBg),
                borderColor: names.map((_, i) => i < names.length - 1 ? DOMAIN_COLORS[i % DOMAIN_COLORS.length] : C.teal),
                borderWidth: 1,
                borderRadius: 6,
                borderSkipped: false,
            }],
        },
        options: domainBarOptions('$'),
    });
}

function renderDomainSends(domains) {
    const names = domains.map(d => d.name);
    const vals  = domains.map(d => d.sends);
    const total = vals.reduce((a,b) => a+b, 0);
    names.push('Grand Total');
    vals.push(total);

    makeChart('chart-domain-sends', {
        type: 'bar',
        data: {
            labels: names,
            datasets: [{
                label: 'Sends',
                data: vals,
                backgroundColor: names.map((_, i) => i < names.length - 1 ? DOMAIN_BG[i % DOMAIN_BG.length] : C.tealBg),
                borderColor: names.map((_, i) => i < names.length - 1 ? DOMAIN_COLORS[i % DOMAIN_COLORS.length] : C.teal),
                borderWidth: 1,
                borderRadius: 6,
                borderSkipped: false,
            }],
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false },
                tooltip: {
                    callbacks: { label: ctx => 'Sends: ' + parseInt(ctx.parsed.y).toLocaleString() }
                }
            },
            scales: {
                x: { grid: gridStyle, ticks: { maxRotation: 45 } },
                y: { grid: gridStyle, beginAtZero: true,
                     ticks: { callback: v => v >= 1000 ? (v/1000).toFixed(0)+'k' : v }
                },
            },
        },
    });
}

function renderDomainLeads(domains) {
    const names = domains.map(d => d.name);
    names.push('Grand Total');

    const soldLeads  = domains.map(d => d.conversions);
    const totalLeads = domains.map(d => d.total_leads);
    soldLeads.push(soldLeads.reduce((a,b)=>a+b,0));
    totalLeads.push(totalLeads.reduce((a,b)=>a+b,0));

    makeChart('chart-domain-leads', {
        type: 'bar',
        data: {
            labels: names,
            datasets: [
                {
                    label: 'Total Leads',
                    data: totalLeads,
                    backgroundColor: C.cyanBg,
                    borderColor: C.cyan,
                    borderWidth: 1,
                    borderRadius: 4,
                    borderSkipped: false,
                },
                {
                    label: 'Sold Leads',
                    data: soldLeads,
                    backgroundColor: C.purpleBg,
                    borderColor: C.purple,
                    borderWidth: 1,
                    borderRadius: 4,
                    borderSkipped: false,
                },
            ],
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { position: 'top' } },
            scales: {
                x: { grid: gridStyle, ticks: { maxRotation: 45 } },
                y: { grid: gridStyle, beginAtZero: true },
            },
        },
    });
}

// ─── Domain bar chart shared options ─────────────────────────────
function domainBarOptions(suffix) {
    const isMoney = suffix === '$';
    return {
        responsive: true, maintainAspectRatio: false,
        plugins: {
            legend: { display: false },
            tooltip: {
                callbacks: {
                    label: ctx => {
                        const v = ctx.parsed.y;
                        return isMoney ? ctx.dataset.label + ': $' + v.toFixed(2)
                                       : ctx.dataset.label + ': ' + v.toFixed(2) + suffix;
                    }
                }
            },
        },
        scales: {
            x: { grid: gridStyle, ticks: { maxRotation: 45 } },
            y: {
                grid: gridStyle,
                beginAtZero: true,
                ticks: {
                    callback: v => isMoney ? '$' + v.toFixed(2) : v.toFixed(1) + suffix,
                },
            },
        },
    };
}

// ─── Trendline computation (linear regression) ───────────────────
function computeTrendline(data) {
    const n = data.length;
    if (n < 2) return data;
    let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
    for (let i = 0; i < n; i++) {
        sumX  += i;
        sumY  += (data[i] || 0);
        sumXY += i * (data[i] || 0);
        sumX2 += i * i;
    }
    const slope     = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
    const intercept = (sumY - slope * sumX) / n;
    return data.map((_, i) => parseFloat((slope * i + intercept).toFixed(2)));
}

// ─── Gradient helper for bar charts ──────────────────────────────
function createGradientBar(canvasId, color1, color2) {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return color1;
    const ctx = canvas.getContext('2d');
    const gradient = ctx.createLinearGradient(0, 0, 0, canvas.height || 300);
    gradient.addColorStop(0, color1);
    gradient.addColorStop(1, color2);
    return gradient;
}

// ─── Utility helpers ─────────────────────────────────────────────
function el(id)    { return document.getElementById(id); }
function fmt(n)    { return parseInt(n || 0).toLocaleString(); }
function money(n)  { return parseFloat(n || 0).toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}); }

function showLoading(on) {
    el('loading').style.display = on ? 'flex' : 'none';
    if (on) {
        el('charts-section').style.display = 'none';
        el('kpi-bar').style.display = 'none';
        el('revenue-bar').style.display = 'none';
    }
}

function showError(msg) {
    const e = el('error-message');
    e.textContent = msg;
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
        btn.querySelector('.btn-label').textContent = 'Sync & Refresh';
    }
}
