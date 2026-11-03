import { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';

import {
  DATASETS,
  formatCurrency,
  formatLastModified,
  formatNumber,
} from '../services/data/transferData';

type CampoAlterado = { campo: string; registros: number };
type Totais = { campo: string; antes: number; depois: number };
type SituacaoContagem = { situacao: string; antes: number; depois: number };

type EntradaHistorico = {
  dataset: string;
  executado_em: string;
  sem_base_anterior: boolean;
  registros: { antes: number | null; depois: number };
  novos: { total: number; exemplos: string[] };
  removidos: { total: number; exemplos: string[] };
  colunas_novas: string[];
  colunas_removidas: string[];
  campos_alterados: CampoAlterado[];
  totais: Totais[];
  situacoes: SituacaoContagem[];
  situacoes_novas: string[];
};

type Historico = { atualizado_em: string | null; entradas: EntradaHistorico[] };

type Estado = { dados?: Historico; carregando: boolean; erro?: string };

const TODAS = 'todas';

export function HistoricoPage() {
  const [estado, setEstado] = useState<Estado>({ carregando: true });
  const [aba, setAba] = useState<string>(TODAS);
  const [aberta, setAberta] = useState<string | null>(null);

  useEffect(() => {
    // Sem cache: o arquivo muda a cada execução do workflow.
    fetch('/data/historico.json', { cache: 'no-store' })
      .then((response) => {
        if (response.status === 404) return { atualizado_em: null, entradas: [] } as Historico;
        if (!response.ok) throw new Error('Não foi possível carregar o histórico.');
        return response.json() as Promise<Historico>;
      })
      .then((dados) => setEstado({ dados, carregando: false }))
      .catch((erro: Error) => setEstado({ carregando: false, erro: erro.message }));
  }, []);

  const entradas = useMemo(() => {
    const todas = estado.dados?.entradas ?? [];
    const filtradas = aba === TODAS ? todas : todas.filter((e) => e.dataset === aba);
    return [...filtradas].sort((a, b) => b.executado_em.localeCompare(a.executado_em));
  }, [estado.dados, aba]);

  const titulo = (id: string) => DATASETS.find((d) => d.id === id)?.shortTitle ?? id;

  return (
    <section className="page detail-page">
      <nav className="breadcrumb" aria-label="Navegação">
        <Link className="back-link" to="/">Início</Link>
        <span className="breadcrumb-sep" aria-hidden="true">›</span>
        <span className="breadcrumb-current">Histórico de atualizações</span>
      </nav>

      <div className="detail-header">
        <div>
          <h1>Histórico de atualizações</h1>
          <p>O que mudou em cada coleta, comparando com a execução anterior. Mantém os últimos 90 dias.</p>
          {estado.dados?.atualizado_em ? (
            <span className="source-label">Última gravação: {formatLastModified(estado.dados.atualizado_em)}</span>
          ) : null}
        </div>
      </div>

      {estado.erro ? (
        <div className="error-panel">{estado.erro}</div>
      ) : estado.carregando ? (
        <div className="loading-panel">Carregando histórico...</div>
      ) : (
        <section className="panel">
          <div className="history-tabs" role="tablist" aria-label="Base">
            {[TODAS, ...DATASETS.map((d) => d.id)].map((id) => (
              <button
                key={id}
                type="button"
                role="tab"
                aria-selected={aba === id}
                className={aba === id ? 'active' : ''}
                onClick={() => {
                  setAba(id);
                  setAberta(null);
                }}
              >
                {id === TODAS ? 'Todas' : titulo(id)}
              </button>
            ))}
          </div>

          {entradas.length === 0 ? (
            <p className="chart-empty">
              Ainda não há execuções registradas para esta base. O histórico começa na próxima atualização do workflow.
            </p>
          ) : (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Execução</th>
                    <th>Base</th>
                    <th>Registros</th>
                    <th>Novos</th>
                    <th>Removidos</th>
                    <th>Campos alterados</th>
                  </tr>
                </thead>
                <tbody>
                  {entradas.map((entrada) => {
                    const chave = `${entrada.dataset}-${entrada.executado_em}`;
                    const expandida = aberta === chave;
                    return (
                      <FragmentoEntrada
                        key={chave}
                        entrada={entrada}
                        rotulo={titulo(entrada.dataset)}
                        expandida={expandida}
                        onToggle={() => setAberta(expandida ? null : chave)}
                      />
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </section>
      )}
    </section>
  );
}

function FragmentoEntrada({
  entrada,
  rotulo,
  expandida,
  onToggle,
}: {
  entrada: EntradaHistorico;
  rotulo: string;
  expandida: boolean;
  onToggle: () => void;
}) {
  const { registros } = entrada;
  const variacao = registros.antes === null ? null : registros.depois - registros.antes;

  return (
    <>
      <tr className="history-row" onClick={onToggle}>
        <td>
          <button type="button" className="link-button" aria-expanded={expandida}>
            {formatLastModified(entrada.executado_em)}
          </button>
        </td>
        <td>{rotulo}</td>
        <td>
          {registros.antes === null
            ? formatNumber(registros.depois)
            : `${formatNumber(registros.antes)} → ${formatNumber(registros.depois)} (${variacao! >= 0 ? '+' : ''}${formatNumber(variacao!)})`}
        </td>
        <td>{entrada.sem_base_anterior ? '—' : formatNumber(entrada.novos.total)}</td>
        <td>{entrada.sem_base_anterior ? '—' : formatNumber(entrada.removidos.total)}</td>
        <td>
          {entrada.sem_base_anterior
            ? 'Primeira execução'
            : entrada.campos_alterados.length
              ? formatNumber(entrada.campos_alterados.length)
              : 'Nenhum'}
        </td>
      </tr>
      {expandida ? (
        <tr className="history-detail">
          <td colSpan={6}>
            <DetalheEntrada entrada={entrada} />
          </td>
        </tr>
      ) : null}
    </>
  );
}

function DetalheEntrada({ entrada }: { entrada: EntradaHistorico }) {
  if (entrada.sem_base_anterior) {
    return <p>Primeira execução registrada: não havia base anterior para comparar.</p>;
  }

  const nada =
    entrada.novos.total === 0 &&
    entrada.removidos.total === 0 &&
    entrada.campos_alterados.length === 0 &&
    entrada.colunas_novas.length === 0 &&
    entrada.colunas_removidas.length === 0;

  return (
    <div className="history-detail-body">
      {nada ? <p>Nenhuma mudança em relação à execução anterior.</p> : null}

      {entrada.novos.total > 0 ? (
        <p>
          <strong>{formatNumber(entrada.novos.total)} novos registros</strong>
          {entrada.novos.exemplos.length ? ` (ex.: ${entrada.novos.exemplos.join(', ')})` : ''}
        </p>
      ) : null}

      {entrada.removidos.total > 0 ? (
        <p>
          <strong>{formatNumber(entrada.removidos.total)} registros removidos</strong>
          {entrada.removidos.exemplos.length ? ` (ex.: ${entrada.removidos.exemplos.join(', ')})` : ''}
        </p>
      ) : null}

      {entrada.colunas_novas.length || entrada.colunas_removidas.length ? (
        <p>
          {entrada.colunas_novas.length ? <>Colunas novas: {entrada.colunas_novas.join(', ')}. </> : null}
          {entrada.colunas_removidas.length ? <>Colunas removidas: {entrada.colunas_removidas.join(', ')}.</> : null}
        </p>
      ) : null}

      {entrada.campos_alterados.length ? (
        <>
          <h3>Campos alterados</h3>
          <table>
            <thead>
              <tr>
                <th>Campo</th>
                <th>Registros alterados</th>
              </tr>
            </thead>
            <tbody>
              {entrada.campos_alterados.map((c) => (
                <tr key={c.campo}>
                  <td>{c.campo}</td>
                  <td>{formatNumber(c.registros)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      ) : null}

      {entrada.totais.length ? (
        <>
          <h3>Totais monetários</h3>
          <table>
            <thead>
              <tr>
                <th>Campo</th>
                <th>Anterior</th>
                <th>Atual</th>
              </tr>
            </thead>
            <tbody>
              {entrada.totais.map((t) => (
                <tr key={t.campo}>
                  <td>{t.campo}</td>
                  <td>{formatCurrency(t.antes)}</td>
                  <td>{formatCurrency(t.depois)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      ) : null}

      {entrada.situacoes.length ? (
        <>
          <h3>Situações</h3>
          <table>
            <thead>
              <tr>
                <th>Situação</th>
                <th>Anterior</th>
                <th>Atual</th>
              </tr>
            </thead>
            <tbody>
              {entrada.situacoes.map((s) => (
                <tr key={s.situacao}>
                  <td>{s.situacao}</td>
                  <td>{formatNumber(s.antes)}</td>
                  <td>{formatNumber(s.depois)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      ) : null}

      {entrada.situacoes_novas.length ? (
        <p>Situações que apareceram pela primeira vez: {entrada.situacoes_novas.join(', ')}</p>
      ) : null}
    </div>
  );
}
