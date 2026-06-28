import { useEffect, useId, useMemo, useState } from 'react';
import { Link, Navigate, useParams } from 'react-router-dom';

import {
  aggregateBy,
  buildCsv,
  filterRows,
  formatCurrency,
  formatNumber,
  getDatasetConfig,
  loadDataset,
  summarizeDataset,
  toNumber,
  uniqueOptions,
  type DatasetConfig,
  type DatasetId,
  type TransferRecord,
} from '../services/data/transferData';

type DataState = {
  rows: TransferRecord[];
  loading: boolean;
  error?: string;
};

const initialDataState: DataState = { rows: [], loading: true };

export function DatasetDetailPage() {
  const { datasetId } = useParams();
  const config = getDatasetConfig(datasetId as DatasetId);

  if (!config) {
    return <Navigate to="/" replace />;
  }

  return <DatasetView config={config} />;
}

function DatasetView({ config }: { config: DatasetConfig }) {
  const [dataById, setDataById] = useState<Record<string, DataState>>({});
  const [years, setYears] = useState<string[]>([]);
  const [statuses, setStatuses] = useState<string[]>([]);
  const [groups, setGroups] = useState<string[]>([]);
  const [search, setSearch] = useState('');
  const [sortColumn, setSortColumn] = useState('');
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('desc');
  const [selectedColumns, setSelectedColumns] = useState<string[]>([]);

  const currentState = dataById[config.id] ?? initialDataState;

  useEffect(() => {
    if (dataById[config.id]) return;

    loadDataset(config)
      .then((rows) => setDataById((state) => ({ ...state, [config.id]: { rows, loading: false } })))
      .catch((error: Error) =>
        setDataById((state) => ({ ...state, [config.id]: { rows: [], loading: false, error: error.message } })),
      );
  }, [config, dataById]);

  useEffect(() => {
    setYears([]);
    setStatuses([]);
    setGroups([]);
    setSearch('');
    setSortColumn('');
    setSelectedColumns([]);
  }, [config.id]);

  const filteredRows = useMemo(
    () => filterRows(currentState.rows, { years, statuses, groups, search }, config),
    [config, currentState.rows, groups, search, statuses, years],
  );

  const orderedRows = useMemo(() => {
    const visibleRows = filteredRows.slice();
    if (!sortColumn) return visibleRows;

    return visibleRows.sort((a, b) => {
      const aNumber = toNumber(a[sortColumn]);
      const bNumber = toNumber(b[sortColumn]);
      const numeric = aNumber !== 0 || bNumber !== 0;
      const result = numeric
        ? aNumber - bNumber
        : String(a[sortColumn] ?? '').localeCompare(String(b[sortColumn] ?? ''));
      return sortDirection === 'asc' ? result : -result;
    });
  }, [filteredRows, sortColumn, sortDirection]);

  const summary = useMemo(() => summarizeDataset(config, filteredRows), [config, filteredRows]);
  const topGroups = useMemo(
    () => aggregateBy(filteredRows, config.groupColumn, config.valueColumn, 8),
    [config, filteredRows],
  );

  const availableColumns = config.tableColumns.filter((column) =>
    currentState.rows.some((row) => Object.prototype.hasOwnProperty.call(row, column)),
  );
  const visibleColumns = selectedColumns.length
    ? selectedColumns.filter((column) => availableColumns.includes(column))
    : availableColumns;

  const maxGroupValue = Math.max(...topGroups.map((item) => item.value), 1);
  const maxYearValue = Math.max(...summary.yearRows.map((item) => item.value), 1);

  return (
    <section className="page detail-page">
      <nav className="breadcrumb" aria-label="Navegação">
        <Link className="back-link" to="/">
          Início
        </Link>
        <span className="breadcrumb-sep" aria-hidden="true">›</span>
        <span className="breadcrumb-current">{config.shortTitle}</span>
      </nav>

      <div className="detail-header">
        <div>
          <h1>{config.title}</h1>
          <p>{config.description}</p>
          <span className="source-label">Fonte: {config.source}</span>
        </div>
      </div>

      {currentState.error ? (
        <div className="error-panel">{currentState.error}</div>
      ) : currentState.loading ? (
        <div className="loading-panel">Carregando planilha...</div>
      ) : (
        <>
          <div className="filter-panel">
            <MultiSelect
              label="Anos"
              options={uniqueOptions(currentState.rows, config.yearColumn)}
              value={years}
              onChange={setYears}
            />
            <MultiSelect
              label="Situacoes"
              options={uniqueOptions(currentState.rows, config.statusColumn)}
              value={statuses}
              onChange={setStatuses}
            />
            <MultiSelect
              label={config.groupColumn.replaceAll('_', ' ')}
              options={uniqueOptions(currentState.rows, config.groupColumn)}
              value={groups}
              onChange={setGroups}
            />
            <label>
              Buscar na planilha
              <input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Municipio, orgao, codigo..." />
            </label>
          </div>

          <div className="kpi-grid">
            <Metric label={config.countLabel} value={formatNumber(summary.totalRecords)} />
            <Metric label={config.valueLabel} value={formatCurrency(summary.totalValue)} />
            <Metric
              label={config.id === 'especiais' ? 'Investimento' : config.id === 'fundo-a-fundo' ? 'Saldo disponivel' : 'Valor pago'}
              value={formatCurrency(summary.secondaryValue)}
            />
            <Metric label="Anos filtrados" value={formatNumber(summary.yearCount)} />
          </div>

          <div className="dashboard-grid">
            <section className="panel">
              <h2>Ranking por {config.groupColumn.replaceAll('_', ' ')}</h2>
              <div className="bar-list">
                {topGroups.map((item) => (
                  <div className="bar-row detailed" key={item.label}>
                    <span>{item.label}</span>
                    <div className="bar-track">
                      <div className="bar-fill warm" style={{ width: `${Math.max((item.value / maxGroupValue) * 100, 3)}%` }} />
                    </div>
                    <strong>{formatCurrency(item.value)}</strong>
                  </div>
                ))}
              </div>
            </section>

            <section className="panel">
              <h2>Evolucao por ano</h2>
              <div className="column-chart">
                {summary.yearRows.map((item) => (
                  <div className="column-item" key={item.label}>
                    <div className="column-track">
                      <span style={{ height: `${Math.max((item.value / maxYearValue) * 100, 4)}%` }} />
                    </div>
                    <small>{item.label}</small>
                  </div>
                ))}
              </div>
            </section>
          </div>

          <section className="panel spreadsheet-panel">
            <div className="panel-title-row">
              <div>
                <h2>Visualizacao da planilha</h2>
                <p>{formatNumber(orderedRows.length)} registros filtrados</p>
              </div>
              <button
                className="secondary-button"
                type="button"
                onClick={() => {
                  setYears([]);
                  setStatuses([]);
                  setGroups([]);
                  setSearch('');
                  setSelectedColumns([]);
                }}
              >
                Limpar filtros
              </button>
              <button
                className="secondary-button"
                type="button"
                onClick={() => downloadRows(config.id, orderedRows, visibleColumns)}
              >
                Baixar CSV
              </button>
            </div>

            <div className="spreadsheet-controls">
              <MultiSelect
                label="Colunas visiveis"
                options={availableColumns}
                value={selectedColumns}
                onChange={setSelectedColumns}
              />
              <div className="filter-summary">
                <strong>Filtros ativos</strong>
                <span>{years.length ? `Anos: ${years.join(', ')}` : 'Anos: todos'}</span>
                <span>{statuses.length ? `Situacoes: ${statuses.length}` : 'Situacoes: todas'}</span>
                <span>{groups.length ? `${config.groupColumn.replaceAll('_', ' ')}: ${groups.length}` : 'Agrupamento: todos'}</span>
              </div>
            </div>

            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    {visibleColumns.map((column) => (
                      <th key={column}>
                        <button
                          type="button"
                          onClick={() => {
                            setSortColumn(column);
                            setSortDirection((direction) =>
                              sortColumn === column && direction === 'desc' ? 'asc' : 'desc',
                            );
                          }}
                        >
                          {column.replaceAll('_', ' ')}
                        </button>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {orderedRows.slice(0, 150).map((row, rowIndex) => (
                    <tr key={`${config.id}-${rowIndex}`}>
                      {visibleColumns.map((column) => (
                        <td key={column}>{formatCell(row[column], column)}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        </>
      )}
    </section>
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

function formatCell(value: unknown, column: string) {
  if (column.startsWith('valor_') || column === 'saldo_disponivel') {
    return formatCurrency(toNumber(value));
  }

  return String(value ?? '');
}

function MultiSelect({
  label,
  options,
  value,
  onChange,
}: {
  label: string;
  options: string[];
  value: string[];
  onChange: (value: string[]) => void;
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const controlId = useId();
  const selectedSet = useMemo(() => new Set(value), [value]);
  const filteredOptions = useMemo(
    () => options.filter((option) => option.toLowerCase().includes(query.trim().toLowerCase())),
    [options, query],
  );

  function toggleOption(option: string) {
    if (selectedSet.has(option)) {
      onChange(value.filter((item) => item !== option));
      return;
    }

    onChange([...value, option]);
  }

  return (
    <div className="multi-select">
      <div className="multi-select-label" id={`${controlId}-label`}>
        {label}
      </div>
      <button
        aria-expanded={open}
        aria-labelledby={`${controlId}-label`}
        className="multi-select-trigger"
        type="button"
        onClick={() => setOpen((current) => !current)}
      >
        <span>{value.length ? `${value.length} selecionado${value.length > 1 ? 's' : ''}` : 'Todos'}</span>
        <strong>{open ? 'Fechar' : 'Selecionar'}</strong>
      </button>

      {value.length > 0 ? (
        <div className="selected-chips" aria-label={`${label} selecionados`}>
          {value.slice(0, 5).map((item) => (
            <button key={item} type="button" onClick={() => toggleOption(item)}>
              {item}
              <span aria-hidden="true">x</span>
            </button>
          ))}
          {value.length > 5 ? <em>+{value.length - 5}</em> : null}
          <button className="clear-chip" type="button" onClick={() => onChange([])}>
            Limpar
          </button>
        </div>
      ) : (
        <small>Sem selecao aplica todos.</small>
      )}

      {open ? (
        <div className="multi-select-menu">
          <input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder={`Buscar ${label.toLowerCase()}`}
          />
          <div className="multi-select-actions">
            <button type="button" onClick={() => onChange(filteredOptions)}>
              Selecionar visiveis
            </button>
            <button type="button" onClick={() => onChange([])}>
              Limpar
            </button>
          </div>
          <div className="multi-select-options">
            {filteredOptions.map((option) => (
              <button
                className={selectedSet.has(option) ? 'selected' : ''}
                key={option}
                type="button"
                onClick={() => toggleOption(option)}
              >
                <span>{option}</span>
                {selectedSet.has(option) ? <strong>Selecionado</strong> : null}
              </button>
            ))}
          </div>
        </div>
      ) : null}
    </div>
  );
}

function downloadRows(datasetId: DatasetId, rows: TransferRecord[], columns: string[]) {
  const blob = new Blob([buildCsv(rows, columns)], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = `${datasetId}-filtrado.csv`;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}
