import { useCallback, useEffect, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/recoincr'

type Row = Record<string, string | number | null>
type ChangeRow = Record<string, string>
type BatchRow = Record<string, unknown>

function post(path: string, body?: Record<string, unknown>) {
  return request(path, { method: 'POST', body: JSON.stringify(body ?? {}) })
}

export default function RecoincrView() {
  const [changes, setChanges] = useState<ChangeRow[]>([])
  const [batches, setBatches] = useState<BatchRow[]>([])
  const [hits, setHits] = useState<Row[]>([])
  const [versions, setVersions] = useState<Row[]>([])
  const [changeInput, setChangeInput] = useState('')
  const [scopeInput, setScopeInput] = useState('')
  const [drift, setDrift] = useState<Record<string, unknown> | null>(null)
  const [message, setMessage] = useState('')
  const [errorMessage, setErrorMessage] = useState('')

  const reload = useCallback(async () => {
    setErrorMessage('')
    try {
      const [changeResp, batchResp, hitResp, versionResp] = await Promise.all([
        request(`${ENDPOINT}/changes?size=200`),
        request(`${ENDPOINT}/batches?size=200`),
        request(`${ENDPOINT}/hits?size=200`),
        request(`${ENDPOINT}/versions?size=200`),
      ])
      if (!changeResp.ok || !batchResp.ok || !hitResp.ok || !versionResp.ok) {
        throw new Error('增量重算数据读取失败')
      }
      const changeJson = await changeResp.json()
      const batchJson = await batchResp.json()
      const hitJson = await hitResp.json()
      const versionJson = await versionResp.json()
      setChanges(changeJson.items ?? [])
      setBatches(batchJson.items ?? [])
      setHits(hitJson.items ?? [])
      setVersions(versionJson.items ?? [])
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '增量重算数据读取失败')
    }
  }, [])

  useEffect(() => {
    void reload()
  }, [reload])

  async function handleAction(call: () => Promise<Response>, successText: string) {
    setErrorMessage('')
    setMessage('')
    try {
      const response = await call()
      const payload = await response.json()
      if (!response.ok || payload.ok === false) {
        throw new Error(payload.message ?? payload.detail ?? '操作未生效，请稍后重试')
      }
      setMessage(payload.message ?? successText)
      await reload()
    } catch (error) {
      setMessage('')
      setErrorMessage(error instanceof Error ? error.message : '增量重算操作失败')
    }
  }

  function registerChange() {
    const scene = changeInput.trim()
    if (!scene) {
      setErrorMessage('请填写推荐场景后再登记变化记录')
      return
    }
    void handleAction(
      () => post(`${ENDPOINT}/changes`, { values: { 推荐场景: scene, 变化类型: '冷启动策略调整' } }),
      '推荐场景变化记录已登记',
    ).then(() => setChangeInput(''))
  }

  function submitBatch() {
    const changeNos = scopeInput
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean)
    void handleAction(
      () => post(`${ENDPOINT}/batches`, { values: changeNos.length ? { change_nos: changeNos } : {} }),
      '局部重算已提交',
    )
  }

  async function showDrift(batchNo: string) {
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/batches/${batchNo}/drift`)
      if (!response.ok) {
        throw new Error('漂移对比读取失败')
      }
      setDrift(await response.json())
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '漂移对比读取失败')
    }
  }

  const affectedScenes = new Set<string>()
  batches.forEach((batch) => {
    ;(batch.受影响推荐场景 as string[] | undefined)?.forEach((scene) => affectedScenes.add(scene))
  })

  return (
    <section className="page" data-module="recoincr">
      <header className="page-head">
        <div>
          <h2>推荐场景增量命中重算</h2>
          <p className="page-desc">
            按场景边界识别受影响候选做局部收敛：仅重算引用变化场景的策略，未变化场景保留原推荐结果，
            同业务记录并发只允许一次有效写入，命中明细按数据版本可回查。
          </p>
        </div>
      </header>

      <div className="stat-row">
        <article className="stat-card">
          <span className="stat-label">场景变化记录</span>
          <strong className="stat-value">{changes.length}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">重算批次</span>
          <strong className="stat-value">{batches.length}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">命中明细数据集</span>
          <strong className="stat-value">{hits.length}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">已覆盖受影响场景</span>
          <strong className="stat-value">{affectedScenes.size}</strong>
        </article>
      </div>

      <form
        className="filter-bar"
        onSubmit={(event) => {
          event.preventDefault()
          submitBatch()
        }}
      >
        <label className="filter-item">
          <span>增量变化单（逗号分隔，留空取全部待处理）</span>
          <input
            value={scopeInput}
            placeholder="如 CHG-0001"
            onChange={(event) => setScopeInput(event.target.value)}
          />
        </label>
        <button className="btn primary" type="submit">提交局部重算</button>
      </form>

      <form
        className="filter-bar"
        onSubmit={(event) => {
          event.preventDefault()
          registerChange()
        }}
      >
        <label className="filter-item">
          <span>登记推荐场景变化记录</span>
          <input
            value={changeInput}
            placeholder="输入发生变化的推荐场景"
            onChange={(event) => setChangeInput(event.target.value)}
          />
        </label>
        <button className="btn" type="submit">登记变化记录</button>
      </form>

      <h3>局部重算批次</h3>
      <table className="data-table">
        <thead>
          <tr>
            <th>批次号</th>
            <th>数据版本</th>
            <th>受影响推荐场景</th>
            <th>批次状态</th>
            <th>重算/保留明细</th>
            <th>已应用</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          {batches.map((batch) => (
            <tr key={String(batch.批次号)}>
              <td>{String(batch.批次号 ?? '—')}</td>
              <td>{String(batch.数据版本 ?? '—')}</td>
              <td>{Array.isArray(batch.受影响推荐场景) ? (batch.受影响推荐场景 as string[]).join('、') : '—'}</td>
              <td>{String(batch.批次状态 ?? '—')}</td>
              <td>
                {String(batch.重算明细数 ?? 0)} / {String(batch.保留明细数 ?? 0)}
              </td>
              <td>{batch.已应用 ? `是（更新${String(batch.更新策略数 ?? 0)}条）` : '否'}</td>
              <td className="row-actions">
                <button className="link" type="button" onClick={() => void showDrift(String(batch.批次号))}>
                  漂移对比
                </button>
                <button
                  className="link"
                  type="button"
                  onClick={() =>
                    void handleAction(
                      () => post(`${ENDPOINT}/batches/${encodeURIComponent(String(batch.批次号))}/retry`),
                      '重试完成',
                    )
                  }
                >
                  重试补写
                </button>
                <button
                  className="link"
                  type="button"
                  onClick={() =>
                    void handleAction(
                      () => post(`${ENDPOINT}/batches/${encodeURIComponent(String(batch.批次号))}/apply`),
                      '已应用到策略库',
                    )
                  }
                >
                  应用到策略库
                </button>
              </td>
            </tr>
          ))}
          {batches.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={7}>暂无局部重算批次</td>
            </tr>
          )}
        </tbody>
      </table>

      {drift && (
        <article className="stat-card" style={{ margin: '12px 0' }}>
          <h3>漂移对比（{String(drift.批次号)}）</h3>
          <p>
            结论：{String(drift.结论)}；未变化场景漂移量：{String(drift.未变化场景漂移量)}；
            评审中配置是否替换上线场景：{drift.评审中配置是否替换上线场景 ? '是' : '否'}
          </p>
          <p>
            受影响场景明细 {Array.isArray(drift.受影响明细) ? (drift.受影响明细 as unknown[]).length : 0} 条，
            未受影响场景明细 {Array.isArray(drift.未受影响明细) ? (drift.未受影响明细 as unknown[]).length : 0} 条
          </p>
        </article>
      )}

      <h3>推荐场景增量命中明细数据集（仅状态确定记录）</h3>
      <table className="data-table">
        <thead>
          <tr>
            <th>数据版本</th>
            <th>批次号</th>
            <th>推荐场景</th>
            <th>策略编号</th>
            <th>命中状态</th>
            <th>策略评分</th>
            <th>复用次数</th>
            <th>评估范围</th>
            <th>当前推荐</th>
          </tr>
        </thead>
        <tbody>
          {hits.map((hit) => (
            <tr key={String(hit.id)}>
              <td>{String(hit.数据版本 ?? '—')}</td>
              <td>{String(hit.批次号 ?? '—')}</td>
              <td>{String(hit.推荐场景 ?? '—')}</td>
              <td>{String(hit.策略编号 ?? '—')}</td>
              <td>{String(hit.命中状态 ?? '—')}</td>
              <td>{String(hit.策略评分 ?? '—')}</td>
              <td>{String(hit.复用次数 ?? '—')}</td>
              <td>{String(hit.评估范围 ?? '—')}</td>
              <td>{hit.是否当前推荐 ? '是' : '历史版本'}</td>
            </tr>
          ))}
          {hits.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={9}>暂无状态确定的命中明细</td>
            </tr>
          )}
        </tbody>
      </table>

      <h3>数据版本回查</h3>
      <table className="data-table">
        <thead>
          <tr>
            <th>数据版本</th>
            <th>批次号</th>
            <th>生成时间</th>
            <th>受影响场景</th>
            <th>更新策略数</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          {versions.map((snapshot) => (
            <tr key={String(snapshot.数据版本)}>
              <td>{String(snapshot.数据版本 ?? '—')}</td>
              <td>{String(snapshot.批次号 ?? '—')}</td>
              <td>{String(snapshot.生成时间 ?? '—')}</td>
              <td>
                {Array.isArray(snapshot.受影响推荐场景)
                  ? (snapshot.受影响推荐场景 as string[]).join('、')
                  : '—'}
              </td>
              <td>{String(snapshot.更新策略数 ?? 0)}</td>
              <td className="row-actions">
                <button
                  className="link"
                  type="button"
                  onClick={() =>
                    void handleAction(
                      () => post(`${ENDPOINT}/versions/${encodeURIComponent(String(snapshot.数据版本))}/rollback`),
                      '已按数据版本回滚',
                    )
                  }
                >
                  回滚到此版本
                </button>
              </td>
            </tr>
          ))}
          {versions.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={6}>暂无数据版本快照</td>
            </tr>
          )}
        </tbody>
      </table>

      <footer className="page-foot">
        <span>受影响场景与已完成推荐结果分别保留，重试仅补写未完成场景</span>
        {message && <span className="error-text" style={{ color: '#1a7f37' }}>{message}</span>}
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
