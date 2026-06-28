import { useEffect, useMemo, useState } from 'react';

import { DashboardPreview } from '../components/dashboard/DashboardPreview';
import {
  DATASETS,
  formatCurrency,
  formatNumber,
  loadDataset,
  loadManifest,
  summarizeDataset,
  type DataManifest,
  type DatasetId,
  type DatasetSummary,
} from '../services/data/transferData';

type SummaryState = Partial<Record<DatasetId, DatasetSummary>>;

export function HomePage() {
  const [summaries, setSummaries] = useState<SummaryState>({});
  const [manifest, setManifest] = useState<DataManifest>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    loadManifest().then(setManifest);

    Promise.all(
      DATASETS.map((config) =>
        loadDataset(config).then((rows) => [config.id, summarizeDataset(config, rows)] as const),
      ),
    )
      .then((entries) => {
        setSummaries(Object.fromEntries(entries) as SummaryState);
      })
      .catch((requestError: Error) => setError(requestError.message))
      .finally(() => setLoading(false));
  }, []);

  const totals = useMemo(() => {
    const allSummaries = Object.values(summaries);
    return {
      records: allSummaries.reduce((total, summary) => total + summary.totalRecords, 0),
      value: allSummaries.reduce((total, summary) => total + summary.totalValue, 0),
    };
  }, [summaries]);

  return (
    <section className="page home-page">
      <div className="home-hero">
        <p className="eyebrow">Transferencias parlamentares — Tocantins</p>
        <h1>Dashboard BI Operacional da SEPLAN</h1>
        <p className="hero-desc">
          Visualizacao integrada dos dados extraidos pelos coletores Python, com acesso a dashboards
          detalhados, filtros interativos e exportacao de planilhas.
        </p>

        <div className="stat-bar" aria-label="Resumo geral">
          <div className="stat-item">
            <span>Registros carregados</span>
            <strong>{loading ? '—' : formatNumber(totals.records)}</strong>
          </div>
          <div className="stat-item">
            <span>Valor consolidado</span>
            <strong>{loading ? '—' : formatCurrency(totals.value)}</strong>
          </div>
          <div className="stat-item">
            <span>Modulos de dados</span>
            <strong>{DATASETS.length}</strong>
          </div>
        </div>
      </div>

      {error ? <div className="error-panel">{error}</div> : null}

      <div className="section-header">
        <h2>Modulos disponiveis</h2>
      </div>

      <div className="preview-grid">
        {DATASETS.map((config) => (
          <DashboardPreview
            config={config}
            key={config.id}
            loading={loading}
            summary={summaries[config.id]}
            lastModified={manifest[config.id]}
          />
        ))}
      </div>
    </section>
  );
}
