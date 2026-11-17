import { useEffect, useMemo, useState, type ReactNode } from 'react';

import { CategoryDonut } from '../components/charts/CategoryDonut';
import { RankingBarChart } from '../components/charts/RankingBarChart';
import { YearEvolutionChart } from '../components/charts/YearEvolutionChart';
import { MultiSelect } from '../components/ui/MultiSelect';
import {
  calcularIndicadores,
  carregarModulo,
  COLUNA_REGISTRO,
  FILTROS_PLANILHA,
  GRUPOS_MODULO,
  NOMES_DATASETS,
  type DadosModulo,
  type GraficoModulo,
  type IndicadoresModulo,
  type NomeDataset,
} from '../services/data/especiaisModulo';
import {
  aggregateBy,
  aggregateByYear,
  aggregateShare,
  buildCsv,
  formatCurrency,
  formatLastModified,
  formatNumber,
  type TransferRecord,
} from '../services/data/transferData';

/** Quantas linhas a tabela desenha de uma vez. O CSV sempre traz o conjunto filtrado inteiro. */
const LINHAS_NA_TELA = 150;
/** Colunas visíveis por padrão, para a planilha não abrir com 40 colunas. */
const COLUNAS_PADRAO = 12;
/** Fatia agregada pelo donut quando há muitas categorias: não é uma categoria real, não filtra. */
const ROTULO_OUTROS = 'Outros';

type EstadoPlanilha = {
  conjunto: NomeDataset;
  filtros: Record<string, string[]>;
  busca: string;
  colunas: string[];
};

const ESTADO_PLANILHA_INICIAL: EstadoPlanilha = {
  conjunto: 'planos_acao',
  filtros: {},
  busca: '',
  colunas: [],
};

type Estado = {
  carregando: boolean;
  erro?: string;
  dados?: DadosModulo;
  geradoEm?: string;
};

/** Módulo completo de transferências especiais: indicadores, grupos, gráficos clicáveis e planilha. */
export function TransfEspeciaisPage() {
  const [estado, setEstado] = useState<Estado>({ carregando: true });
  // Estado da planilha fica aqui para os gráficos poderem preencher os filtros dela.
  const [planilha, setPlanilha] = useState<EstadoPlanilha>(ESTADO_PLANILHA_INICIAL);

  useEffect(() => {
    let ativo = true;
    carregarModulo()
      .then(({ dados, geradoEm }) => {
        if (ativo) setEstado({ carregando: false, dados, geradoEm });
      })
      .catch((erro: Error) => {
        if (ativo) setEstado({ carregando: false, erro: erro.message });
      });
    return () => {
      ativo = false;
    };
  }, []);

  const dados = estado.dados;
  const indicadores = useMemo(() => (dados ? calcularIndicadores(dados) : undefined), [dados]);

  /** Clique num gráfico: liga o filtro daquela categoria na planilha (liga/desliga). */
  const filtrarPeloGrafico = (grafico: GraficoModulo, valor: string) => {
    setPlanilha((atual) => {
      if (atual.conjunto !== grafico.dataset) {
        return { conjunto: grafico.dataset, filtros: { [grafico.agrupar]: [valor] }, busca: '', colunas: [] };
      }
      const atuais = atual.filtros[grafico.agrupar] ?? [];
      const proximos = atuais.includes(valor) ? atuais.filter((item) => item !== valor) : [...atuais, valor];
      return { ...atual, filtros: { ...atual.filtros, [grafico.agrupar]: proximos } };
    });
  };

  const selecionadosDo = (grafico: GraficoModulo) =>
    planilha.conjunto === grafico.dataset ? (planilha.filtros[grafico.agrupar] ?? []) : [];

  return (
    <section className="page transf-especiais-page">
      <div className="home-hero">
        <p className="eyebrow">Transferências especiais — Tocantins</p>
        <h1>Módulo completo de transferências especiais</h1>
        <p className="hero-desc">
          {NOMES_DATASETS.length} conjuntos de dados da API pública do Transferegov, organizados em{' '}
          {GRUPOS_MODULO.length} grupos. Os gráficos usam os dados da última coleta.
        </p>
        {estado.geradoEm ? <p className="hero-desc">Coleta de {formatLastModified(estado.geradoEm)}</p> : null}
      </div>

      {estado.erro ? <div className="error-panel">{estado.erro}</div> : null}
      {estado.carregando ? <div className="loading-panel">Carregando módulo...</div> : null}

      {indicadores ? <Indicadores indicadores={indicadores} /> : null}

      {dados ? (
        <p className="panel-hint">Clique numa barra, fatia ou ano para filtrar a planilha no fim da página.</p>
      ) : null}

      {dados
        ? GRUPOS_MODULO.map((grupo) => (
            <section className="transf-grupo" id={grupo.id} key={grupo.id}>
              <div className="section-header">
                <h2>{grupo.titulo}</h2>
              </div>
              <p className="panel-hint">{grupo.descricao}</p>
              <div className="dashboard-grid halves">
                {grupo.graficos.map((grafico) => (
                  <GraficoPanel
                    key={grafico.id}
                    grafico={grafico}
                    dados={dados}
                    selecionados={selecionadosDo(grafico)}
                    aoSelecionar={(valor) => filtrarPeloGrafico(grafico, valor)}
                  />
                ))}
              </div>
            </section>
          ))
        : null}

      {dados ? (
        <PlanilhaModulo
          dados={dados}
          estado={planilha}
          onChange={(parcial) => setPlanilha((atual) => ({ ...atual, ...parcial }))}
        />
      ) : null}
    </section>
  );
}

function Indicadores({ indicadores }: { indicadores: IndicadoresModulo }) {
  const itens: Array<{ rotulo: string; valor: string }> = [
    { rotulo: 'Planos de ação', valor: formatNumber(indicadores.planos) },
    { rotulo: 'Valor dos planos', valor: formatCurrency(indicadores.valorPlanos) },
    { rotulo: 'Empenhado', valor: formatCurrency(indicadores.empenhado) },
    { rotulo: 'Documentos hábeis', valor: formatCurrency(indicadores.valorDh) },
    { rotulo: 'Lançamentos financeiros', valor: formatCurrency(indicadores.valorLancamentos) },
    { rotulo: 'Saldo atual das contas', valor: formatCurrency(indicadores.saldoAtual) },
  ];

  return (
    <div className="kpi-grid" aria-label="Indicadores gerais">
      {itens.map((item) => (
        <div className="metric-card" key={item.rotulo}>
          <span>{item.rotulo}</span>
          <strong>{item.valor}</strong>
        </div>
      ))}
    </div>
  );
}

function GraficoPanel({
  grafico,
  dados,
  selecionados,
  aoSelecionar,
}: {
  grafico: GraficoModulo;
  dados: DadosModulo;
  selecionados: string[];
  aoSelecionar: (valor: string) => void;
}) {
  const linhas = dados[grafico.dataset] ?? [];
  // Sem coluna para somar, o gráfico conta registros (a coluna sintética vale 1 em cada linha).
  const coluna = grafico.somar ?? COLUNA_REGISTRO;
  const formato = grafico.somar ? 'moeda' : 'numero';
  const valorLabel = grafico.somar ? 'Valor' : 'Registros';
  const clicar = (valor: string) => {
    if (valor !== ROTULO_OUTROS) aoSelecionar(valor);
  };

  let conteudo: ReactNode;
  if (grafico.tipo === 'barras') {
    conteudo = (
      <RankingBarChart
        rows={aggregateBy(linhas, grafico.agrupar, coluna, grafico.limite ?? 10)}
        valueLabel={valorLabel}
        formato={formato}
        selected={selecionados}
        onSelect={clicar}
      />
    );
  } else if (grafico.tipo === 'rosca') {
    // A rosca distribui por contagem de registros, então o total é de registros.
    const { slices, total } = aggregateShare(linhas, grafico.agrupar, coluna);
    conteudo = (
      <CategoryDonut
        slices={slices}
        total={total}
        totalLabel="registros"
        selected={selecionados}
        onSelect={clicar}
      />
    );
  } else {
    conteudo = (
      <YearEvolutionChart
        rows={aggregateByYear(linhas, grafico.agrupar, coluna)}
        valueLabel={valorLabel}
        formato={formato}
        selected={selecionados}
        onSelect={clicar}
      />
    );
  }

  return (
    <section className="panel">
      <div className="panel-heading">
        <h2>{grafico.titulo}</h2>
        <span className="panel-hint">{grafico.dica}</span>
      </div>
      {conteudo}
    </section>
  );
}

function textoCelula(valor: unknown) {
  if (valor === undefined || valor === null || valor === '' || valor === 'nan') return 'Não informado';
  return String(valor).trim();
}

/**
 * Planilha do módulo, no mesmo formato do painel de especiais: escolha do conjunto,
 * filtros de múltipla escolha, busca, colunas visíveis e download do CSV filtrado.
 * O estado vem da página, porque os gráficos também preenchem os filtros.
 */
function PlanilhaModulo({
  dados,
  estado,
  onChange,
}: {
  dados: DadosModulo;
  estado: EstadoPlanilha;
  onChange: (parcial: Partial<EstadoPlanilha>) => void;
}) {
  const { conjunto, filtros, busca, colunas: colunasEscolhidas } = estado;

  const linhas = dados[conjunto] ?? [];
  const colunasDisponiveis = useMemo(
    () => Object.keys(linhas[0] ?? {}).filter((coluna) => coluna !== COLUNA_REGISTRO),
    [linhas],
  );
  const colunasVisiveis = useMemo(() => {
    const validas = colunasEscolhidas.filter((coluna) => colunasDisponiveis.includes(coluna));
    return validas.length ? validas : colunasDisponiveis.slice(0, COLUNAS_PADRAO);
  }, [colunasEscolhidas, colunasDisponiveis]);

  const filtrosDoConjunto = FILTROS_PLANILHA[conjunto] ?? [];

  const opcoesDoFiltro = (coluna: string) =>
    [...new Set(linhas.map((linha) => textoCelula(linha[coluna])))].sort((a, b) => a.localeCompare(b, 'pt-BR'));

  const filtradas = useMemo(() => {
    const termo = busca.trim().toLowerCase();
    return linhas.filter((linha) => {
      for (const [coluna, valores] of Object.entries(filtros)) {
        if (valores.length > 0 && !valores.includes(textoCelula(linha[coluna]))) return false;
      }
      if (termo && !colunasDisponiveis.some((coluna) => String(linha[coluna] ?? '').toLowerCase().includes(termo))) {
        return false;
      }
      return true;
    });
  }, [linhas, filtros, busca, colunasDisponiveis]);

  const baixarCsv = () => {
    const blob = new Blob([buildCsv(filtradas as TransferRecord[], colunasVisiveis)], {
      type: 'text/csv;charset=utf-8',
    });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `${conjunto}-filtrado.csv`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
  };

  const filtrosAtivos = Object.entries(filtros).filter(([, valores]) => valores.length > 0);

  return (
    <section className="panel spreadsheet-panel" id="planilha">
      <div className="panel-title-row">
        <div>
          <h2>Visualização da planilha</h2>
          <p>
            {formatNumber(filtradas.length)} registros filtrados de {formatNumber(linhas.length)}
          </p>
        </div>
        <button
          className="secondary-button"
          type="button"
          onClick={() => onChange({ filtros: {}, busca: '', colunas: [] })}
        >
          Limpar filtros
        </button>
        <button className="secondary-button" type="button" onClick={baixarCsv} disabled={!filtradas.length}>
          Baixar CSV
        </button>
      </div>

      <div className="spreadsheet-controls">
        <label className="multi-select planilha-campo">
          <span className="multi-select-label">Conjunto de dados</span>
          <select
            value={conjunto}
            onChange={(evento) =>
              onChange({ conjunto: evento.target.value as NomeDataset, filtros: {}, busca: '', colunas: [] })
            }
          >
            {NOMES_DATASETS.map((nome) => (
              <option key={nome} value={nome}>
                {nome.replaceAll('_', ' ')}
              </option>
            ))}
          </select>
        </label>

        {filtrosDoConjunto.map((filtro) => (
          <MultiSelect
            key={`${conjunto}-${filtro.coluna}`}
            label={filtro.rotulo}
            options={opcoesDoFiltro(filtro.coluna)}
            value={filtros[filtro.coluna] ?? []}
            onChange={(valores) => onChange({ filtros: { ...filtros, [filtro.coluna]: valores } })}
          />
        ))}

        <label className="multi-select planilha-campo">
          <span className="multi-select-label">Buscar na planilha</span>
          <input
            type="search"
            value={busca}
            onChange={(evento) => onChange({ busca: evento.target.value })}
            placeholder="Qualquer texto"
          />
        </label>

        <MultiSelect
          label="Colunas visíveis"
          options={colunasDisponiveis}
          value={colunasVisiveis}
          onChange={(colunas) => onChange({ colunas })}
        />

        <div className="filter-summary">
          <strong>Filtros ativos</strong>
          {filtrosAtivos.length === 0 && !busca ? <span>Nenhum filtro ativo</span> : null}
          {filtrosAtivos.map(([coluna, valores]) => {
            const rotulo = filtrosDoConjunto.find((filtro) => filtro.coluna === coluna)?.rotulo ?? coluna.replaceAll('_', ' ');
            return (
              <span key={coluna}>
                {rotulo}: {valores.join(', ')}
              </span>
            );
          })}
          {busca ? <span>Busca: {busca}</span> : null}
        </div>
      </div>

      {linhas.length === 0 ? (
        <p className="chart-empty">Este conjunto não tem registros na última coleta.</p>
      ) : (
        <>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  {colunasVisiveis.map((coluna) => (
                    <th key={coluna}>{coluna.replaceAll('_', ' ')}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {filtradas.slice(0, LINHAS_NA_TELA).map((linha, indice) => (
                  <tr key={`${conjunto}-${indice}`}>
                    {colunasVisiveis.map((coluna) => (
                      <td key={coluna}>{textoCelula(linha[coluna])}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {filtradas.length > LINHAS_NA_TELA ? (
            <p className="panel-hint">
              Mostrando {formatNumber(LINHAS_NA_TELA)} de {formatNumber(filtradas.length)} linhas. O CSV traz o conjunto filtrado inteiro.
            </p>
          ) : null}
        </>
      )}
    </section>
  );
}
