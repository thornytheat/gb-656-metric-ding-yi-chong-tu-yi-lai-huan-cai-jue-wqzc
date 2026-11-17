/* 表单与请求：动作集中在 ACTIONS 里，失败必须写到页面上 */
const RELOAD_AFTER = new Set(['run', 'dl-create', 'dl-generate', 'dl-claim', 'dl-renew', 'dl-cancel']);

function leaseKey(taskId) {
  return `dl-lease:${taskId}`;
}

function saveLease(taskId, seq, token) {
  try {
    window.localStorage.setItem(leaseKey(taskId), JSON.stringify({ seq, token }));
  } catch (error) { /* 隐私模式下静默降级，续传按钮会提示先领取 */ }
}

function loadLease(taskId) {
  try {
    const raw = window.localStorage.getItem(leaseKey(taskId));
    return raw ? JSON.parse(raw) : null;
  } catch (error) {
    return null;
  }
}

const ACTIONS = {
  async import() {
    return request('/api/schemas/import', {
      files: ['schema_v2026_06.sql', 'schema_v2026_09.sql'],
    });
  },

  async run(button) {
    const body = {};
    if (button.dataset.forceStale) {
      body.base_version_seq = 0;
    }
    const run = await request('/api/runs', body);
    return run.reused
      ? `命中已有批次 ${run.run_id}`
      : `新批次 ${run.run_id} 序号 ${run.version_seq}`;
  },

  async 'compare-types'() {
    const oldValue = document.getElementById('old-type');
    const newValue = document.getElementById('new-type');
    const result = await request('/api/types/compare', {
      old: oldValue ? oldValue.value : '',
      new: newValue ? newValue.value : '',
    });
    const target = document.querySelector('[data-compare-result]');
    if (target) {
      const detail = result.detail ? `/${result.detail}` : '';
      target.textContent = `${result.old.norm} → ${result.new.norm}　${result.change}${detail}　${result.breaking ? '破坏' : '兼容'}`;
    }
    return `比较完成 ${result.change}`;
  },

  async 'dl-create'() {
    const sizeInput = document.getElementById('dl-fragment-bytes');
    const keyInput = document.getElementById('dl-idem-key');
    const body = {};
    if (sizeInput && sizeInput.value) {
      body.fragment_bytes = Number(sizeInput.value);
    }
    if (keyInput && keyInput.value.trim()) {
      body.idem_key = keyInput.value.trim();
    }
    const task = await request('/api/downloads', body);
    return task.reused ? `命中已有任务 ${task.task_id}` : `新任务 ${task.task_id}`;
  },

  async 'dl-generate'(button) {
    const task = await request(`/api/downloads/${button.dataset.task}/generate`, {});
    return `生成 ${task.generated_bytes}/${task.total_bytes} 字节`;
  },

  async 'dl-claim'(button) {
    const taskId = button.dataset.task;
    const lease = await request(`/api/downloads/${taskId}/claim`, {});
    saveLease(taskId, lease.seq, lease.lease_token);
    return `已领取片段 ${lease.seq}，${lease.bytes} 字节，租约至 ${lease.lease_expires_at}`;
  },

  async 'dl-resume'(button) {
    const taskId = button.dataset.task;
    const lease = loadLease(taskId);
    if (!lease) {
      throw new Error('先领取一个片段再续传');
    }
    const result = await request(`/api/downloads/${taskId}/resume`, {
      seq: lease.seq,
      lease_token: lease.token,
    });
    return `续传片段 ${result.seq}，${result.bytes} 字节，租约至 ${result.lease_expires_at}`;
  },

  async 'dl-renew'(button) {
    const taskId = button.dataset.task;
    const lease = loadLease(taskId);
    if (!lease) {
      throw new Error('先领取一个片段再续租');
    }
    const result = await request(`/api/downloads/${taskId}/renew`, {
      seq: lease.seq,
      lease_token: lease.token,
    });
    return `片段 ${result.seq} 租约延至 ${result.lease_expires_at}`;
  },

  async 'dl-cancel'(button) {
    const task = await request(`/api/downloads/${button.dataset.task}/cancel`, {});
    return `任务 ${task.task_id} 已取消`;
  },
};

async function request(url, body) {
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body || {}),
  });
  const text = await response.text();
  let payload = null;
  try {
    payload = text ? JSON.parse(text) : null;
  } catch (error) {
    payload = null;
  }
  if (!response.ok) {
    const message = (payload && (payload.message || payload.error)) || `HTTP ${response.status}`;
    throw new Error(message);
  }
  return payload;
}

function flash(target, message, bad) {
  if (!target) return;
  target.textContent = message;
  target.classList.toggle('is-bad', Boolean(bad));
  target.classList.toggle('is-ok', !bad);
}

document.addEventListener('click', async (event) => {
  const button = event.target.closest('[data-action]');
  if (!button) return;
  const name = button.dataset.action;
  const handler = ACTIONS[name];
  if (!handler) return;
  const target = document.querySelector('[data-flash]');
  button.disabled = true;
  flash(target, '…', false);
  try {
    const message = await handler(button);
    flash(target, message || '完成', false);
    if (RELOAD_AFTER.has(name)) {
      setTimeout(() => window.location.reload(), 400);
    }
  } catch (error) {
    flash(target, error.message, true);
  } finally {
    button.disabled = false;
  }
});
