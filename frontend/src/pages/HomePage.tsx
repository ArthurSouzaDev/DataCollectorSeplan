import { useEffect, useMemo, useState } from 'react';

import { DashboardPreview } from '../components/dashboard/DashboardPreview';
import {
  DATASETS,
  loadDataset,
  summarizeDataset,
  type DatasetId,
  type DatasetSummary,
} from '../services/data/transferData';

type SummaryState = Partial<Record<DatasetId, DatasetSummary>>;

export function HomePage() {
  const [summaries, setSummaries] = useState<SummaryState>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
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
      <div className="hero">
        <div>
          <p className="eyebrow">Transferencias parlamentares TO</p>
          <h1>Dashboard BI operacional da SEPLAN</h1>
          <p>
            Pre-visualizacao dos dados extraidos pelos coletores Python, com acesso a paginas detalhadas,
            filtros interativos e visualizacao da planilha.
          </p>
        </div>
        <div className="hero-summary" aria-label="Resumo geral">
          <span>Registros carregados</span>
          <strong>{new Intl.NumberFormat('pt-BR').format(totals.records)}</strong>
          <span>Valor consolidado</span>
          <strong>
            {new Intl.NumberFormat('pt-BR', {
              style: 'currency',
              currency: 'BRL',
              maximumFractionDigits: 0,
            }).format(totals.value)}
          </strong>
        </div>
      </div>

      {error ? <div className="error-panel">{error}</div> : null}

      <div className="preview-grid">
        {DATASETS.map((config) => (
          <DashboardPreview
            config={config}
            key={config.id}
            loading={loading}
            summary={summaries[config.id]}
          />
        ))}
      </div>
    </section>
  );
}
