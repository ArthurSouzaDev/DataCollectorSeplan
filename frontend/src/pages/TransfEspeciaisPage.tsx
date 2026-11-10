import { useEffect, useMemo, useState, type ReactNode } from 'react';

import { CategoryDonut } from '../components/charts/CategoryDonut';
import { RankingBarChart } from '../components/charts/RankingBarChart';
import { YearEvolutionChart } from '../components/charts/YearEvolutionChart';
import {
  calcularIndicadores,
  carregarModulo,
  COLUNA_REGISTRO,
  GRUPOS_MODULO,
  NOMES_DATASETS,
  type DadosModulo,
  type GraficoModulo,
  type IndicadoresModulo,
} from '../services/data/especiaisModulo';
import {
  aggregateBy,
  aggregateByYear,
  aggregateShare,
  formatCurrency,
  formatLastModified,
  formatNumber,
} from '../services/data/transferData';

type Estado = {
  carregando: boolean;
  erro?: string;
  dados?: DadosModulo;
  geradoEm?: string;
};

/** Módulo completo de transferências especiais: indicadores, grupos e gráficos por dataset. */
export function TransfEspeciaisPage() {
  const [estado, setEstado] = useState<Estado>({ carregando: true });

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

      {dados
        ? GRUPOS_MODULO.map((grupo) => (
            <section className="transf-grupo" id={grupo.id} key={grupo.id}>
              <div className="section-header">
                <h2>{grupo.titulo}</h2>
              </div>
              <p className="panel-hint">{grupo.descricao}</p>
              <div className="dashboard-grid halves">
                {grupo.graficos.map((grafico) => (
                  <GraficoPanel key={grafico.id} grafico={grafico} dados={dados} />
                ))}
              </div>
            </section>
          ))
        : null}
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

function GraficoPanel({ grafico, dados }: { grafico: GraficoModulo; dados: DadosModulo }) {
  const linhas = dados[grafico.dataset] ?? [];
  // Sem coluna para somar, o gráfico conta registros (a coluna sintética vale 1 em cada linha).
  const coluna = grafico.somar ?? COLUNA_REGISTRO;
  const formato = grafico.somar ? 'moeda' : 'numero';
  const valorLabel = grafico.somar ? 'Valor' : 'Registros';

  let conteudo: ReactNode;
  if (grafico.tipo === 'barras') {
    conteudo = (
      <RankingBarChart
        rows={aggregateBy(linhas, grafico.agrupar, coluna, grafico.limite ?? 10)}
        valueLabel={valorLabel}
        formato={formato}
      />
    );
  } else if (grafico.tipo === 'rosca') {
    // A rosca distribui por contagem de registros, então o total é de registros.
    const { slices, total } = aggregateShare(linhas, grafico.agrupar, coluna);
    conteudo = <CategoryDonut slices={slices} total={total} totalLabel="registros" />;
  } else {
    conteudo = (
      <YearEvolutionChart
        rows={aggregateByYear(linhas, grafico.agrupar, coluna)}
        valueLabel={valorLabel}
        formato={formato}
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
