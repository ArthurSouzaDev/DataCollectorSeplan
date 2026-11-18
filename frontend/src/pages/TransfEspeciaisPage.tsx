import { createPortal } from 'react-dom';
import { memo, useCallback, useDeferredValue, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';

import { CategoryDonut } from '../components/charts/CategoryDonut';
import { RankingBarChart } from '../components/charts/RankingBarChart';
import { YearEvolutionChart } from '../components/charts/YearEvolutionChart';
import { FichaRegistro, type Ancora } from '../components/charts/FichaRegistro';
import { MultiSelect } from '../components/ui/MultiSelect';
import {
  calcularIndicadores,
  carregarModulo,
  COLUNA_REGISTRO,
  aplicarFiltros,
  FILTROS_PLANILHA,
  filtrarDados,
  type FiltrosPorConjunto,
  GRUPOS_MODULO,
  NOMES_DATASETS,
  textoCelula,
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
/** Tempo mínimo em que a telinha de carregamento fica na tela após cada filtro. */
const TEMPO_TELINHA_MS = 1500;
const SEM_LINHAS: TransferRecord[] = [];
const SEM_SELECAO: string[] = [];
/** Fatia agregada pelo donut quando há muitas categorias: não é uma categoria real, não filtra. */
const ROTULO_OUTROS = 'Outros';

type EstadoPlanilha = {
  conjunto: NomeDataset;
  busca: string;
  colunas: string[];
};

const ESTADO_PLANILHA_INICIAL: EstadoPlanilha = {
  conjunto: 'planos_acao',
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
  // Filtros por conjunto de dados: valem para os gráficos e a planilha daquele conjunto.
  const [filtros, setFiltros] = useState<FiltrosPorConjunto>({});
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
  // O recálculo dos filtros é adiado: o clique aparece na hora (os checkboxes e o botão
  // respondem) e os gráficos atualizam logo em seguida, com a mensagem "Atualizando".
  const filtrosAdiados = useDeferredValue(filtros);
  const atualizando = filtros !== filtrosAdiados;
  // A telinha aparece a cada filtro e fica pelo menos TEMPO_TELINHA_MS, para o usuário perceber.
  const [telinhaVisivel, setTelinhaVisivel] = useState(false);
  const filtrosAnteriores = useRef(filtros);
  const temporizador = useRef<number | undefined>(undefined);
  useEffect(() => {
    if (filtrosAnteriores.current === filtros) return;
    filtrosAnteriores.current = filtros;
    setTelinhaVisivel(true);
    window.clearTimeout(temporizador.current);
    temporizador.current = window.setTimeout(() => setTelinhaVisivel(false), TEMPO_TELINHA_MS);
  }, [filtros]);
  useEffect(() => () => window.clearTimeout(temporizador.current), []);
  const mostrarTelinha = atualizando || telinhaVisivel;
  const dadosFiltrados = useMemo(
    () => (dados ? filtrarDados(dados, filtrosAdiados) : undefined),
    [dados, filtrosAdiados],
  );
  const indicadores = useMemo(
    () => (dadosFiltrados ? calcularIndicadores(dadosFiltrados) : undefined),
    [dadosFiltrados],
  );

  /**
   * Clique num gráfico: liga/desliga o filtro daquela categoria no conjunto do gráfico.
   * Como no painel de especiais, o filtro vale para todos os gráficos e para a planilha
   * desse conjunto. A planilha passa a mostrar esse conjunto.
   */
  // Estável (só usa setters), para os gráficos memorizados não redesenharem à toa.
  const filtrarPeloGrafico = useCallback((grafico: GraficoModulo, valor: string) => {
    setPlanilha((atual) =>
      atual.conjunto === grafico.dataset
        ? atual
        : { conjunto: grafico.dataset, busca: '', colunas: [] },
    );
    setFiltros((atual) => {
      const doConjunto = atual[grafico.dataset] ?? {};
      const atuais = doConjunto[grafico.agrupar] ?? [];
      const proximos = atuais.includes(valor) ? atuais.filter((item) => item !== valor) : [...atuais, valor];
      return { ...atual, [grafico.dataset]: { ...doConjunto, [grafico.agrupar]: proximos } };
    });
  }, []);

  const selecionadosDo = (grafico: GraficoModulo) => filtros[grafico.dataset]?.[grafico.agrupar] ?? SEM_SELECAO;

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

      {mostrarTelinha
        ? // Portal no body: um ancestral com transform faria o "fixed" ficar no meio da página, não da tela.
          createPortal(
            <div className="filter-loading" role="status" aria-live="polite">
              Atualizando gráficos e planilha...
            </div>,
            document.body,
          )
        : null}

      {dados
        ? GRUPOS_MODULO.map((grupo) => (
            <section className="transf-grupo" id={grupo.id} key={grupo.id}>
              <div className="section-header">
                <h2>{grupo.titulo}</h2>
              </div>
              <p className="panel-hint">{grupo.descricao}</p>
              <div className="dashboard-grid halves">
                {grupo.graficos.map((grafico) => (
                  <GraficoPanelMemo
                    key={grafico.id}
                    grafico={grafico}
                    linhas={(dadosFiltrados ?? dados)[grafico.dataset] ?? SEM_LINHAS}
                    selecionados={selecionadosDo(grafico)}
                    aoSelecionar={filtrarPeloGrafico}
                  />
                ))}
              </div>
            </section>
          ))
        : null}

      {dados ? (
        <PlanilhaModulo
          dados={dados}
          dadosFiltrados={dadosFiltrados ?? dados}
          filtros={filtros}
          onFiltros={setFiltros}
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
  linhas,
  selecionados,
  aoSelecionar,
}: {
  grafico: GraficoModulo;
  linhas: TransferRecord[];
  selecionados: string[];
  aoSelecionar: (grafico: GraficoModulo, valor: string) => void;
}) {
  // Sem coluna para somar, o gráfico conta registros (a coluna sintética vale 1 em cada linha).
  const coluna = grafico.somar ?? COLUNA_REGISTRO;
  const formato = grafico.somar ? 'moeda' : 'numero';
  const valorLabel = grafico.somar ? 'Valor' : 'Registros';
  const clicar = (valor: string) => {
    if (valor !== ROTULO_OUTROS) aoSelecionar(grafico, valor);
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

const GraficoPanelMemo = memo(GraficoPanel);

/**
 * Planilha do módulo, no mesmo formato do painel de especiais: escolha do conjunto,
 * filtros de múltipla escolha, busca, colunas visíveis e download do CSV filtrado.
 * Os filtros são os mesmos que os gráficos usam: clicar num gráfico preenche estes campos.
 */
function PlanilhaModulo({
  dados,
  dadosFiltrados,
  filtros,
  onFiltros,
  estado,
  onChange,
}: {
  dados: DadosModulo;
  dadosFiltrados: DadosModulo;
  filtros: FiltrosPorConjunto;
  onFiltros: (atualizar: (atual: FiltrosPorConjunto) => FiltrosPorConjunto) => void;
  estado: EstadoPlanilha;
  onChange: (parcial: Partial<EstadoPlanilha>) => void;
}) {
  const { conjunto, busca, colunas: colunasEscolhidas } = estado;
  const [ficha, setFicha] = useState<{ row: TransferRecord; ancora: Ancora } | null>(null);

  const linhas = dados[conjunto] ?? [];
  const valoresFiltro = filtros[conjunto] ?? {};
  const colunasDisponiveis = useMemo(
    () => Object.keys(linhas[0] ?? {}).filter((coluna) => coluna !== COLUNA_REGISTRO),
    [linhas],
  );
  const colunasVisiveis = useMemo(() => {
    const validas = colunasEscolhidas.filter((coluna) => colunasDisponiveis.includes(coluna));
    return validas.length ? validas : colunasDisponiveis.slice(0, COLUNAS_PADRAO);
  }, [colunasEscolhidas, colunasDisponiveis]);

  const filtrosDoConjunto = FILTROS_PLANILHA[conjunto] ?? [];

  // As opções vêm dos dados completos, para não sumirem quando um filtro restringe a lista.
  const opcoesPorFiltro = useMemo(
    () =>
      Object.fromEntries(
        (FILTROS_PLANILHA[conjunto] ?? []).map((filtro) => [
          filtro.coluna,
          [...new Set(linhas.map((linha) => textoCelula(linha[filtro.coluna])))].sort((a, b) =>
            a.localeCompare(b, 'pt-BR'),
          ),
        ]),
      ) as Record<string, string[]>,
    [linhas, conjunto],
  );
  // A busca é adiada para a digitação não travar em tabelas grandes.
  const buscaAdiada = useDeferredValue(busca);

  const definirFiltros = (novos: Record<string, string[]>) =>
    onFiltros((atual) => ({ ...atual, [conjunto]: novos }));

  const filtradas = useMemo(() => {
    const termo = buscaAdiada.trim().toLowerCase();
    const porFiltro = dadosFiltrados[conjunto] ?? [];
    if (!termo) return porFiltro;
    return porFiltro.filter((linha) =>
      colunasDisponiveis.some((coluna) => String(linha[coluna] ?? '').toLowerCase().includes(termo)),
    );
  }, [dadosFiltrados, conjunto, buscaAdiada, colunasDisponiveis]);

  function abrirFicha(linha: TransferRecord, elemento: HTMLElement) {
    const rect = elemento.getBoundingClientRect();
    // A coluna sintética de contagem não faz parte do registro original.
    const registro = Object.fromEntries(Object.entries(linha).filter(([campo]) => campo !== COLUNA_REGISTRO)) as TransferRecord;
    setFicha({ row: registro, ancora: { x: rect.left, y: rect.bottom, topo: rect.top } });
  }

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

  const filtrosAtivos = Object.entries(valoresFiltro).filter(([, valores]) => valores.length > 0);

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
          onClick={() => {
            definirFiltros({});
            onChange({ busca: '', colunas: [] });
          }}
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
            onChange={(evento) => onChange({ conjunto: evento.target.value as NomeDataset, busca: '', colunas: [] })}
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
            options={opcoesPorFiltro[filtro.coluna] ?? []}
            value={valoresFiltro[filtro.coluna] ?? []}
            onChange={(valores) => definirFiltros({ ...valoresFiltro, [filtro.coluna]: valores })}
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
            const rotulo =
              filtrosDoConjunto.find((filtro) => filtro.coluna === coluna)?.rotulo ?? coluna.replaceAll('_', ' ');
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
                  <tr
                    key={`${conjunto}-${indice}`}
                    className="clickable-row"
                    tabIndex={0}
                    onClick={(evento) => abrirFicha(linha, evento.currentTarget)}
                    onKeyDown={(evento) => {
                      if (evento.key === 'Enter') abrirFicha(linha, evento.currentTarget);
                    }}
                  >
                    {colunasVisiveis.map((coluna) => (
                      <td key={coluna}>{textoCelula(linha[coluna])}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {ficha ? (
            <FichaRegistro
              row={ficha.row}
              titulo={String(Object.values(ficha.row)[0] ?? '')}
              situacaoColumn={colunasDisponiveis.find((coluna) => coluna.startsWith('situacao')) ?? ''}
              ancora={ficha.ancora}
              onClose={() => setFicha(null)}
            />
          ) : null}
          {filtradas.length > LINHAS_NA_TELA ? (
            <p className="panel-hint">
              Mostrando {formatNumber(LINHAS_NA_TELA)} de {formatNumber(filtradas.length)} linhas. O CSV traz o conjunto
              filtrado inteiro.
            </p>
          ) : null}
        </>
      )}
    </section>
  );
}
