import { Link } from 'react-router-dom';

import type { DatasetConfig, DatasetSummary } from '../../services/data/transferData';
import { formatCurrency, formatLastModified, formatNumber } from '../../services/data/transferData';

type DashboardPreviewProps = {
  config: DatasetConfig;
  summary?: DatasetSummary;
  loading?: boolean;
  lastModified?: string;
};

export function DashboardPreview({ config, summary, loading = false, lastModified }: DashboardPreviewProps) {
  const maxValue = Math.max(...(summary?.topEntities.map((item) => item.value) ?? [0]), 1);

  return (
    <article className="dashboard-card">
      <div className="card-heading">
        <div>
          <p className="eyebrow">Componente</p>
          <h2>{config.shortTitle}</h2>
        </div>
        <Link className="icon-button" to={`/dashboard/${config.id}`} aria-label={`Abrir ${config.title}`}>
          Ver detalhes
        </Link>
      </div>

      <p className="card-description">{config.description}</p>

      <div className="card-body">
        {loading || !summary ? (
          <div className="loading-panel">Carregando dados...</div>
        ) : (
          <>
            <div className="kpi-grid compact">
              <Metric label={config.countLabel} value={formatNumber(summary.totalRecords)} />
              <Metric label={config.valueLabel} value={formatCurrency(summary.totalValue)} />
              <Metric label="Anos" value={formatNumber(summary.yearCount)} />
            </div>

            <div className="mini-chart" aria-label={`Ranking de ${config.entityColumn}`}>
              {summary.topEntities.map((item, index) => {
                const delay = `${Math.min(index * 0.05, 0.4)}s`;
                return (
                  <div className="bar-row" key={item.label} style={{ animationDelay: delay }}>
                    <span title={item.label}>{item.label}</span>
                    <div className="bar-track">
                      <div
                        className="bar-fill"
                        style={{ width: `${Math.max((item.value / maxValue) * 100, 3)}%`, animationDelay: delay }}
                      />
                    </div>
                    <strong>{formatCurrency(item.value)}</strong>
                  </div>
                );
              })}
            </div>
          </>
        )}
      </div>

      {lastModified ? (
        <p className="card-updated">Atualizado em {formatLastModified(lastModified)}</p>
      ) : null}
    </article>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="metric-card">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}
