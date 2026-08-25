import { Cell, Funnel, FunnelChart, LabelList, ResponsiveContainer, Tooltip } from 'recharts';

import { formatPercent, type ExecutionSummary } from '../../services/data/datasetKpis';
import { formatCurrency } from '../../services/data/transferData';
import { ChartTooltip } from './ChartTooltip';
import { executionColors } from './chartTheme';

type ExecutionPanelProps = {
  execution: ExecutionSummary;
};

/**
 * Execução financeira: funil Repasse → Desembolsado → Pago com o percentual
 * sobre a etapa inicial, mais os cards de execução.
 * Equivale à seção "💹 Execução Financeira" do dashboard Streamlit.
 */
export function ExecutionPanel({ execution }: ExecutionPanelProps) {
  const data = execution.stages.map((stage) => ({
    label: stage.label,
    value: stage.value,
    percentLabel: formatPercent(stage.percentOfInitial),
    valueLabel: formatCurrency(stage.value),
  }));

  const hasValues = data.some((stage) => stage.value > 0);

  return (
    <section className="panel execution-panel">
      <h2>Execução financeira</h2>

      <div className="execution-grid">
        <div className="execution-chart">
          {hasValues ? (
            <ResponsiveContainer width="100%" height={320}>
              <FunnelChart margin={{ top: 12, right: 210, bottom: 12, left: 8 }}>
                <Tooltip content={<ChartTooltip valueLabel="Valor" />} />
                <Funnel
                  dataKey="value"
                  data={data}
                  nameKey="label"
                  isAnimationActive
                  animationDuration={650}
                  lastShapeType="rectangle"
                >
                  {data.map((stage, index) => (
                    <Cell key={stage.label} fill={executionColors[index] ?? executionColors[0]} />
                  ))}
                  <LabelList
                    dataKey="percentLabel"
                    position="center"
                    fill="var(--surface)"
                    stroke="none"
                    fontSize={15}
                    fontWeight={800}
                  />
                  <LabelList
                    dataKey="label"
                    position="right"
                    offset={14}
                    dy={-9}
                    fill="var(--text)"
                    stroke="none"
                    fontSize={12}
                    fontWeight={700}
                  />
                  <LabelList
                    dataKey="valueLabel"
                    position="right"
                    offset={14}
                    dy={9}
                    fill="var(--muted)"
                    stroke="none"
                    fontSize={11}
                  />
                </Funnel>
              </FunnelChart>
            </ResponsiveContainer>
          ) : (
            <p className="chart-empty">Sem valores de execução para os filtros aplicados.</p>
          )}
        </div>

        <div className="execution-cards">
          <ExecutionCard
            label="Desembolsado / repasse"
            value={formatPercent(execution.percentDisbursed)}
            tone="success"
          />
          <ExecutionCard label="Pago / repasse" value={formatPercent(execution.percentPaid)} tone="warm" />
          <ExecutionCard label="Saldo total em conta" value={formatCurrency(execution.saldoConta)} tone="accent" />
        </div>
      </div>
    </section>
  );
}

function ExecutionCard({ label, value, tone }: { label: string; value: string; tone: string }) {
  return (
    <div className={`metric-card tone-${tone}`}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}
