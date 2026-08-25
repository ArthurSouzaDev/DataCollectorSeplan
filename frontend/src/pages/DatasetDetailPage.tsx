import { useEffect, useId, useMemo, useRef, useState } from 'react';
import { Link, Navigate, useParams } from 'react-router-dom';

import {
  aggregateBy,
  aggregateByYear,
  aggregateComposition,
  aggregateShare,
  buildCsv,
  filterRows,
  formatCurrency,
  formatCurrencyShort,
  formatNumber,
  getDatasetConfig,
  loadDataset,
  toNumber,
  uniqueOptions,
  type DatasetConfig,
  type DatasetId,
  type TransferRecord,
} from '../services/data/transferData';
import {
  computeExecution,
  computeKpis,
  supportsExecutionPanel,
  type KpiItem,
} from '../services/data/datasetKpis';
import { CategoryDonut } from '../components/charts/CategoryDonut';
import { CompositionChart } from '../components/charts/CompositionChart';
import { ExecutionPanel } from '../components/charts/ExecutionPanel';
import { RankingBarChart } from '../components/charts/RankingBarChart';
import { YearEvolutionChart } from '../components/charts/YearEvolutionChart';

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
  const [natures, setNatures] = useState<string[]>([]);
  const [onlyFormalized, setOnlyFormalized] = useState(false);
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
    setNatures([]);
    setOnlyFormalized(false);
    setSearch('');
    setSortColumn('');
    setSelectedColumns([]);
  }, [config.id]);

  const filteredRows = useMemo(
    () => filterRows(currentState.rows, { years, statuses, groups, natures, onlyFormalized, search }, config),
    [config, currentState.rows, groups, natures, onlyFormalized, search, statuses, years],
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

  const topGroups = useMemo(
    () => aggregateBy(filteredRows, config.groupColumn, config.valueColumn, 8),
    [config, filteredRows],
  );
  const kpis = useMemo(() => computeKpis(config, filteredRows), [config, filteredRows]);
  const yearRows = useMemo(
    () => aggregateByYear(filteredRows, config.yearColumn, config.valueColumn),
    [config, filteredRows],
  );
  const statusShare = useMemo(
    () => aggregateShare(filteredRows, config.statusColumn, config.valueColumn),
    [config, filteredRows],
  );
  const natureShare = useMemo(
    () => aggregateShare(filteredRows, 'natureza_juridica', config.valueColumn),
    [config, filteredRows],
  );
  const composition = useMemo(
    () => (config.composition ? aggregateComposition(filteredRows, config.composition) : null),
    [config, filteredRows],
  );
  const execution = useMemo(
    () => (supportsExecutionPanel(config) ? computeExecution(filteredRows) : null),
    [config, filteredRows],
  );

  // Cross-filtering: clicar numa barra do ranking aplica (ou remove) o filtro
  // do agrupamento, e todo o painel — KPIs, funil e tabela — reage junto.
  function toggleGroupFilter(label: string) {
    setGroups((current) =>
      current.includes(label) ? current.filter((item) => item !== label) : [...current, label],
    );
  }

  function toggleStatusFilter(label: string) {
    setStatuses((current) =>
      current.includes(label) ? current.filter((item) => item !== label) : [...current, label],
    );
  }

  function toggleNatureFilter(label: string) {
    setNatures((current) =>
      current.includes(label) ? current.filter((item) => item !== label) : [...current, label],
    );
  }

  const availableColumns = config.tableColumns.filter((column) =>
    currentState.rows.some((row) => Object.prototype.hasOwnProperty.call(row, column)),
  );
  const visibleColumns = selectedColumns.length
    ? selectedColumns.filter((column) => availableColumns.includes(column))
    : availableColumns;


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
            <MultiSelect
              label="Natureza juridica"
              options={uniqueOptions(currentState.rows, 'natureza_juridica')}
              value={natures}
              onChange={setNatures}
            />
            <label className="checkbox-filter">
              <input
                type="checkbox"
                checked={onlyFormalized}
                onChange={(event) => setOnlyFormalized(event.target.checked)}
              />
              Somente convenios formalizados
            </label>
            <label>
              Buscar na planilha
              <input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Municipio, orgao, codigo..." />
            </label>
          </div>

          <div className={`kpi-grid kpi-grid-${kpis.length}`}>
            {kpis.map((kpi) => (
              <KpiCard key={kpi.key} kpi={kpi} dense={kpis.length >= 6} />
            ))}
          </div>

          {execution ? <ExecutionPanel execution={execution} /> : null}

          <div className="dashboard-grid">
            <section className="panel">
              <div className="panel-heading">
                <h2>Ranking por {config.groupColumn.replaceAll('_', ' ')}</h2>
                {groups.length ? (
                  <button className="link-button" type="button" onClick={() => setGroups([])}>
                    Limpar seleção
                  </button>
                ) : (
                  <span className="panel-hint">Clique numa barra para filtrar</span>
                )}
              </div>
              <RankingBarChart
                rows={topGroups}
                valueLabel={config.valueLabel}
                selected={groups}
                onSelect={toggleGroupFilter}
              />
            </section>

            <section className="panel">
              <div className="panel-heading">
                <h2>Evolução por ano</h2>
                <span className="panel-hint">Valor e quantidade</span>
              </div>
              <YearEvolutionChart
                rows={yearRows}
                valueLabel={config.valueLabel}
                countLabel={config.countLabel}
              />
            </section>
          </div>

          <div className="dashboard-grid halves">
            <section className="panel">
              <div className="panel-heading">
                <h2>Distribuição por situação</h2>
                {statuses.length ? (
                  <button className="link-button" type="button" onClick={() => setStatuses([])}>
                    Limpar seleção
                  </button>
                ) : (
                  <span className="panel-hint">Clique numa fatia para filtrar</span>
                )}
              </div>
              <CategoryDonut
                slices={statusShare.slices}
                total={statusShare.total}
                totalLabel={config.countLabel.toLowerCase()}
                selected={statuses}
                onSelect={toggleStatusFilter}
              />
            </section>

            <section className="panel">
              <div className="panel-heading">
                <h2>Natureza jurídica</h2>
                {natures.length ? (
                  <button className="link-button" type="button" onClick={() => setNatures([])}>
                    Limpar seleção
                  </button>
                ) : (
                  <span className="panel-hint">Clique numa fatia para filtrar</span>
                )}
              </div>
              <CategoryDonut
                slices={natureShare.slices}
                total={natureShare.total}
                totalLabel={config.countLabel.toLowerCase()}
                selected={natures}
                onSelect={toggleNatureFilter}
              />
            </section>
          </div>

          {composition && config.composition ? (
            <section className="panel">
              <div className="panel-heading">
                <h2>{config.composition.title}</h2>
                <span className="panel-hint">{config.composition.hint}</span>
              </div>
              <CompositionChart rows={composition} config={config.composition} />
            </section>
          ) : null}

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
                  setNatures([]);
                  setOnlyFormalized(false);
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
                <span>{natures.length ? `Natureza juridica: ${natures.length}` : 'Natureza juridica: todas'}</span>
                <span>{onlyFormalized ? 'Somente formalizados: sim' : 'Somente formalizados: nao'}</span>
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

/**
 * Em linhas com 6 ou 7 cartões o valor por extenso não cabe, então o cartão
 * mostra a forma abreviada e mantém o valor exato no title (hover).
 */
function KpiCard({ kpi, dense }: { kpi: KpiItem; dense: boolean }) {
  const exact = kpi.kind === 'count' ? formatNumber(kpi.amount) : formatCurrency(kpi.amount);
  const shown = kpi.kind === 'currency' && dense ? formatCurrencyShort(kpi.amount) : exact;

  return (
    <div className={kpi.tone ? `metric-card tone-${kpi.tone}` : 'metric-card'}>
      <span>{kpi.label}</span>
      <strong title={shown === exact ? undefined : exact}>{shown}</strong>
      {kpi.hint ? <em className="metric-hint">{kpi.hint}</em> : null}
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
  const containerRef = useRef<HTMLDivElement>(null);
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

  useEffect(() => {
    if (!open) return;

    function handleOutsideInteraction(event: PointerEvent) {
      if (!containerRef.current?.contains(event.target as Node)) {
        setOpen(false);
      }
    }

    document.addEventListener('pointerdown', handleOutsideInteraction);
    return () => document.removeEventListener('pointerdown', handleOutsideInteraction);
  }, [open]);

  return (
    <div className="multi-select" ref={containerRef}>
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
