import React, { Fragment, useEffect, useMemo, useState } from 'https://esm.sh/react@18.3.1';
import { createRoot } from 'https://esm.sh/react-dom@18.3.1/client';

const el = React.createElement;

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

    const tableProgress = useMemo(() => run?.table_progress || [], [run]);

    async function loadRuns() {
        const data = await api.get('/api/runs');
        const allRuns = Array.isArray(data) ? data : [];
        setRuns(allRuns);
        if (!selectedRunId && allRuns.length > 0) {
            setSelectedRunId(allRuns[allRuns.length - 1].run_id);
        }
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

    const runList = el('div', { className: 'run-list' },
        ...runs.map((item) => el('div', {
            key: item.run_id,
            className: `run-item ${selectedRunId === item.run_id ? 'active' : ''}`,
            onClick: () => setSelectedRunId(item.run_id),
        },
            el('div', { className: 'run-id' }, item.run_id),
            el('div', { className: 'run-plan' }, item.plan_id),
            el('div', { style: { marginTop: '.4rem' } },
                el('span', { className: `badge ${statusClass(item.status)}` }, item.status),
            ))),
    );

    const details = !run
        ? el('div', { className: 'section' }, 'Select a migration run from the left.')
        : el('div', null,
            el('section', { className: 'hero', id: 'overview' },
                el('h2', null, `Run ${run.run_id}`),
                el('p', null, 'Track lifecycle state and guide migration phases.'),
            ),
            el('section', { className: 'section' },
                el('h3', null, 'Run overview'),
                el('div', { className: 'grid' },
                    el('div', { className: 'metric' }, el('div', { className: 'metric-label' }, 'Lifecycle'), el('div', { className: 'metric-value' }, el('span', { className: `badge ${statusClass(run.status)}` }, run.status))),
                    el('div', { className: 'metric' }, el('div', { className: 'metric-label' }, 'Phase'), el('div', { className: 'metric-value' }, el('span', { className: 'badge warn' }, run.orchestration_phase || 'unknown'))),
                    el('div', { className: 'metric' }, el('div', { className: 'metric-label' }, 'Validation'), el('div', { className: 'metric-value' }, el('span', { className: `badge ${statusClass(run.validation_status)}` }, run.validation_status || 'unknown'))),
                ),
                el('div', { className: 'actions' },
                    el('button', { className: 'primary', onClick: () => doAction('Start orchestration', `/api/runs/${run.run_id}/start`, { max_phases: 1 }) }, 'Start phase'),
                    el('button', { onClick: () => doAction('Pause orchestration', `/api/runs/${run.run_id}/pause`) }, 'Pause'),
                    el('button', { onClick: () => doAction('Resume orchestration', `/api/runs/${run.run_id}/resume`, { max_phases: 1 }) }, 'Resume phase'),
                ),
            ),
            el('section', { className: 'section', id: 'tables' },
                el('h3', null, 'Per-table execution progress'),
                el('div', { className: 'table-wrap' },
                    el('table', { className: 'table' },
                        el('thead', null, el('tr', null,
                            el('th', null, 'Table'),
                            el('th', null, 'Status'),
                            el('th', null, 'Rows copied'),
                            el('th', null, 'Progress'),
                        )),
                        el('tbody', null,
                            ...tableProgress.map((t) => el('tr', { key: t.table_name },
                                el('td', null, t.table_name),
                                el('td', null, el('span', { className: `badge ${statusClass(t.status)}` }, t.status)),
                                el('td', null, String(t.rows_copied ?? 0)),
                                el('td', null,
                                    el('div', { className: 'progress' },
                                        el('span', { style: { width: `${t.progress_percent ?? 0}%` } }),
                                    ),
                                ),
                            )),
                        ),
                    ),
                ),
            ),
            el('section', { className: 'section', id: 'raw' },
                el('h3', null, 'Raw run payload'),
                el('pre', null, JSON.stringify(run, null, 2)),
            ),
        );

    return el('div', { className: 'app' },
        el('div', { className: 'shell' },
            el('header', { className: 'topbar' },
                el('div', { className: 'brand' },
                    el('div', { className: 'brand-mark' }),
                    el('div', null,
                        el('h1', null, 'Migration Copilot'),
                        el('p', null, 'Operational command center'),
                    ),
                ),
                el('div', { className: 'topbar-actions' },
                    el('span', { className: 'pill' }, 'Live control plane'),
                    el('span', { className: `badge ${statusClass(run?.status || '')}` }, run?.status || 'no run selected'),
                ),
            ),
            el('div', { className: 'layout' },
                el('aside', { className: 'sidebar' },
                    el('h2', { className: 'section-title' }, 'Migration runs'),
                    runList,
                ),
                el('main', { className: 'main' },
                    error ? el('div', { className: 'section callout' }, el('b', null, 'Error: '), error) : null,
                    details,
                ),
            ),
        ),
    );
}

createRoot(document.getElementById('root')).render(el(App));
