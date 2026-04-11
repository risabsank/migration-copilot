import React, { useEffect, useMemo, useState } from 'https://esm.sh/react@18.3.1';
import { createRoot } from 'https://esm.sh/react-dom@18.3.1/client';
import htm from 'https://esm.sh/htm@3.1.1';

const html = htm.bind(React.createElement);

const api = {
    get: (path) => fetch(path).then(async (r) => {
        const data = await r.json();
        if (!r.ok) throw new Error(data.detail || 'Request failed');
        return data;
    }),
    post: (path, body = {}) => fetch(path, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
    }).then(async (r) => {
        const data = await r.json();
        if (!r.ok) throw new Error(data.detail || 'Request failed');
        return data;
    }),
};

function statusClass(status) {
    if (!status) return 'warn';
    if (['failed', 'denied', 'blocked', 'rollback_in_progress'].some((x) => status.includes(x))) return 'err';
    if (['completed', 'passed', 'ready', 'cutover_complete'].some((x) => status.includes(x))) return 'ok';
    return 'warn';
}

function App() {
    const [runs, setRuns] = useState([]);
    const [selectedRunId, setSelectedRunId] = useState('');
    const [run, setRun] = useState(null);
    const [error, setError] = useState('');

    const blockers = useMemo(() => run?.lifecycle?.blockers || [], [run]);
    const tableProgress = run?.table_progress || [];
    const validationSummary = run?.validation_summary || {};
    const cutoverEvaluation = run?.cutover_evaluation || { blocking_conditions: [] };
    const rollbackPlan = run?.rollback_plan || { status: 'unknown' };
    const timeline = run?.timeline || [];
    const approvalHistory = run?.approval_history || [];
    const opsHistory = run?.ops_recommendation_history || [];
    const dashboard = run?.dashboard || { health: { healthy: false, slo_status: { status: 'unknown' }, summary: 'n/a' }, table_completion: { completed: 0, total: 0 }, lag_seconds: 0 };

    async function loadRuns() {
        const data = await api.get('/api/runs');
        setRuns(Array.isArray(data) ? data : []);
        if (!selectedRunId && Array.isArray(data) && data.length) setSelectedRunId(data[data.length - 1].run_id);
    }

    async function loadRunDetail(runId) {
        if (!runId) return;
        const data = await api.get(`/api/runs/${runId}`);
        setRun(data);
    }

    useEffect(() => {
        loadRuns().catch((e) => setError(`Load runs failed: ${e.message}`));
    }, []);

    useEffect(() => {
        loadRunDetail(selectedRunId).catch((e) => setError(`Load run detail failed: ${e.message}`));
    }, [selectedRunId]);

    async function doAction(label, path, body = {}) {
        setError('');
        try {
            await api.post(path, body);
            await loadRunDetail(selectedRunId);
            await loadRuns();
        } catch (e) {
            setError(`${label}: ${e.message}`);
        }
    }

    return html`<div className="app">
    <div className="shell">
        <header className="topbar">
          <div className="brand">
            <div className="brand-mark"></div>
            <div>
              <h1>Migration Copilot</h1>
              <p>Operational command center</p>
            </div>
          </div>
          <div className="topbar-actions">
            <span className="pill">Live control plane</span>
            <span className=${`badge ${statusClass(run?.status || '')}`}>${run?.status || 'no run selected'}</span>
          </div>
          </header>

        <div className="layout">
          <aside className="sidebar">
            <h2 className="section-title">Migration runs</h2>
            <div className="run-list">
              ${runs.map((item) => html`<div key=${item.run_id} className=${`run-item ${selectedRunId === item.run_id ? 'active' : ''}`} onClick=${() => setSelectedRunId(item.run_id)}>
                <div className="run-id">${item.run_id}</div>
                <div className="run-plan">${item.plan_id}</div>
                <div style=${{ marginTop: '.4rem' }}>
                  <span className=${`badge ${statusClass(item.status)}`}>${item.status}</span>
                </div>
              </div>`)}
            </div>
          </aside>

          <main className="main">
            ${error ? html`<div className="section callout"><b>Error:</b> ${error}</div>` : null}

            ${!run ? html`<div className="section">Select a migration run from the left.</div>` : html`<>
              <section className="hero" id="overview">
                <h2>Run ${run.run_id}</h2>
                <p>Track lifecycle state, validate readiness, and safely guide each migration phase.</p>
              </section>

              <div className="nav">
                ${['overview', 'tables', 'validation', 'cdc', 'cutover', 'rollback', 'timeline', 'approvals', 'ai', 'incident', 'dashboards'].map((id) => html`<a key=${id} href=${`#${id}`}>${id}</a>`)}
              </div>

              <section className="section">
                <h3>Run overview</h3>
                <div className="grid">
                  <div className="metric"><div className="metric-label">Lifecycle</div><div className="metric-value"><span className=${`badge ${statusClass(run.status)}`}>${run.status}</span></div></div>
                  <div className="metric"><div className="metric-label">Phase</div><div className="metric-value"><span className="badge warn">${run.orchestration_phase}</span></div></div>
                  <div className="metric"><div className="metric-label">Validation</div><div className="metric-value"><span className=${`badge ${statusClass(run.validation_status)}`}>${run.validation_status}</span></div></div>
                  <div className="metric"><div className="metric-label">Cutover</div><div className="metric-value"><span className=${`badge ${run.cutover_ready ? 'ok' : 'warn'}`}>${run.cutover_ready ? 'ready' : 'not ready'}</span></div></div>
                  <div className="metric"><div className="metric-label">Rollback</div><div className="metric-value"><span className=${`badge ${run.rollback_ready ? 'ok' : 'warn'}`}>${run.rollback_ready ? 'ready' : 'not ready'}</span></div></div>
                </div>

                ${blockers.length > 0 ? html`<div className="callout"><b>Blocking conditions</b><ul>${blockers.map((b) => html`<li key=${b}>${b}</li>`)}</ul></div>` : null}

                <div className="actions">
                  <button className="primary" onClick=${() => doAction('Start orchestration', `/api/runs/${run.run_id}/start`, { max_phases: 1 })}>Start phase</button>
                  <button onClick=${() => doAction('Pause orchestration', `/api/runs/${run.run_id}/pause`)}>Pause</button>
                  <button onClick=${() => doAction('Resume orchestration', `/api/runs/${run.run_id}/resume`, { max_phases: 1 })}>Resume phase</button>
                  <button onClick=${() => doAction('Request approval', `/api/runs/${run.run_id}/approvals/request`, { action: 'begin_cutover', actor: 'ui-operator' })}>Request approval</button>
                  <button onClick=${() => doAction('Approve cutover', `/api/runs/${run.run_id}/approvals/decision`, { action: 'begin_cutover', actor: 'ui-operator', approved: true })}>Approve cutover</button>
                  <button onClick=${() => doAction('Deny cutover', `/api/runs/${run.run_id}/approvals/decision`, { action: 'begin_cutover', actor: 'ui-operator', approved: false })}>Deny cutover</button>
                  <button onClick=${() => doAction('Trigger rollback', `/api/runs/${run.run_id}/rollback`, { actor: 'ui-operator', human_approved: true })}>Trigger rollback</button>
                </div>
              </section>

              <section className="section" id="tables">
                <h3>Per-table execution progress</h3>
                <div className="table-wrap">
                  <table className="table">
                    <thead><tr><th>Table</th><th>Status</th><th>Rows copied</th><th>Progress</th><th>Action</th></tr></thead>
                    <tbody>
                      ${tableProgress.map((t) => html`<tr key=${t.table_name}>
                        <td>${t.table_name}</td>
                        <td><span className=${`badge ${statusClass(t.status)}`}>${t.status}</span></td>
                        <td>${t.rows_copied}</td>
                        <td><div className="progress"><span style=${{ width: `${t.progress_percent}%` }}></span></div></td>
                        <td>${t.status === 'failed' ? html`<button onClick=${() => doAction('Retry table', `/api/runs/${run.run_id}/tables/${t.table_name}/retry`, { actor: 'ui-operator', human_approved: true })}>Retry</button>` : '-'}</td>
                      </tr>`)}
                    </tbody>
                  </table>
                </div>
              </section>

              <section className="section" id="validation"><h3>Validation results</h3><div>Total checks: ${validationSummary.total_checks ?? 0} · Passed: ${validationSummary.passed_checks ?? 0} · Failed: ${validationSummary.failed_checks ?? 0}</div></section>
              <section className="section" id="cdc"><h3>CDC / catch-up status</h3><div className="grid"><div className="metric"><div className="metric-label">CDC status</div><div className="metric-value"><span className=${`badge ${statusClass(run.cdc_status)}`}>${run.cdc_status}</span></div></div><div className="metric"><div className="metric-label">Replication lag</div><div className="metric-value">${run.replication_lag_seconds ?? 'n/a'} sec</div></div><div className="metric"><div className="metric-label">Source freshness</div><div className="metric-value">${run.source_freshness_seconds ?? 'n/a'} sec</div></div></div></section>
              <section className="section" id="cutover"><h3>Cutover readiness</h3><ul>${(cutoverEvaluation.blocking_conditions || []).map((b) => html`<li key=${b}>${b}</li>`)}</ul></section>
              <section className="section" id="rollback"><h3>Rollback state</h3><div>Status: <span className=${`badge ${statusClass(rollbackPlan.status)}`}>${rollbackPlan.status}</span></div></section>
              <section className="section" id="timeline"><h3>Event timeline / audit log</h3><div className="table-wrap"><table className="table"><thead><tr><th>Timestamp</th><th>Type</th><th>Description</th></tr></thead><tbody>${timeline.map((e, idx) => html`<tr key=${idx}><td>${e.timestamp}</td><td>${e.event_type}</td><td>${e.description}</td></tr>`)}</tbody></table></div></section>
              <section className="section" id="approvals"><h3>Approvals / policy decisions</h3><pre>${JSON.stringify(approvalHistory, null, 2)}</pre></section>
              <section className="section" id="ai"><h3>AI recommendation summary (advisory)</h3><pre>${JSON.stringify(opsHistory.slice(-3), null, 2)}</pre><div>Policy profile: <b>${run.execution_policy_profile}</b></div></section>
              <section className="section" id="incident"><h3>Incident pack viewer</h3><button onClick=${async () => { const pack = await api.get(`/api/runs/${run.run_id}/incident-pack`); alert(`Incident: ${pack.failure_cause_summary}`); }}>Open incident summary</button></section>
              <section className="section" id="dashboards"><h3>Monitoring dashboard</h3><div className="grid"><div className="metric">Health: <span className=${`badge ${dashboard.health.healthy ? 'ok' : 'err'}`}>${dashboard.health.slo_status.status}</span><div>${dashboard.health.summary}</div></div><div className="metric">Backfill completion<div className="progress"><span style=${{ width: `${(dashboard.table_completion.completed / Math.max(dashboard.table_completion.total, 1)) * 100}%` }}></span></div></div><div className="metric">CDC lag gauge<div className="progress"><span style=${{ width: `${Math.min(Number(dashboard.lag_seconds || 0), 300) / 3}%`, background: '#f59e0b' }}></span></div></div></div></section>            </>`}
          </main>
       </div>
    </div>`;
}

createRoot(document.getElementById('root')).render(html`< ${App} />`);
