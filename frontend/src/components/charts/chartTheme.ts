/**
 * Tema dos gráficos.
 *
 * As cores são referências a CSS custom properties do design system (global.css).
 * O Recharts repassa esses valores direto para atributos SVG (`fill`, `stroke`),
 * então o gráfico troca de cor junto com o tema claro/escuro sem nenhum JS.
 */

export const chartColors = {
  accent: 'var(--accent)',
  accentStrong: 'var(--accent-strong)',
  success: 'var(--success)',
  warm: 'var(--warm)',
  danger: 'var(--danger)',
  muted: 'var(--muted)',
  mutedLight: 'var(--muted-light)',
  border: 'var(--border)',
  surface: 'var(--surface)',
  text: 'var(--text)',
} as const;

/** Cores do funil de execução: repasse → desembolsado → pago. */
export const executionColors = [chartColors.accent, chartColors.success, chartColors.warm] as const;

/**
 * Paleta categórica — 6 posições, ordem fixa, nunca reciclada.
 *
 * A ordem é o mecanismo de segurança para daltonismo: cada par vizinho foi
 * validado com o simulador Machado-Oliveira-Fernandes (protanopia e deuteranopia,
 * severidade 1.0) contra as superfícies reais do tema.
 *
 *   claro  (#ffffff): ΔE CVD mínimo 9,1 · ΔE visão normal mínimo 19,6
 *   escuro (#182232): ΔE CVD mínimo 8,4 · ΔE visão normal mínimo 19,3
 *
 * Três tons do modo claro ficam abaixo de 3:1 de contraste com o branco, então a
 * identidade nunca depende só da cor: toda rosca traz legenda com rótulo, valor e
 * percentual, e a planilha completa fica na mesma página.
 *
 * Os valores vivem em `global.css` (`--series-1`…`--series-6`) para trocarem
 * sozinhos entre claro e escuro.
 */
export const seriesColors = [
  'var(--series-1)',
  'var(--series-2)',
  'var(--series-3)',
  'var(--series-4)',
  'var(--series-5)',
  'var(--series-6)',
] as const;

export function seriesColor(index: number) {
  return seriesColors[index % seriesColors.length];
}

/** Espessura máxima de barra e coluna (o resto da faixa vira respiro). */
export const MAX_BAR_SIZE = 24;

/** Separador de 2px na cor da superfície entre fatias que se tocam. */
export const SURFACE_GAP = 2;

export const axisProps = {
  stroke: chartColors.border,
  tick: { fill: 'var(--muted)', fontSize: 11 },
  tickLine: false,
} as const;

/** Abrevia valores no eixo: 1.2 mi, 340 mil. Eixos com R$ por extenso não cabem. */
export function formatCompactCurrency(value: number) {
  const abs = Math.abs(value);
  const short = (divisor: number, unit: string) => {
    const scaled = value / divisor;
    const digits = Math.abs(scaled) >= 100 ? 0 : 1;
    // ",0" no eixo é ruído: 30 mi lê melhor que 30,0 mi.
    const text = scaled.toFixed(digits).replace(/\.0$/, '').replace('.', ',');
    return `R$ ${text} ${unit}`;
  };

  if (abs >= 1_000_000_000) return short(1_000_000_000, 'bi');
  if (abs >= 1_000_000) return short(1_000_000, 'mi');
  if (abs >= 1_000) return `R$ ${Math.round(value / 1_000)} mil`;
  return `R$ ${Math.round(value)}`;
}

/**
 * Mesma abreviação sem o "R$" e sem decimal redundante — para o eixo Y vertical,
 * onde o rótulo da série já diz que a medida é dinheiro e o espaço é estreito.
 */
export function formatCompactAmount(value: number) {
  return formatCompactCurrency(value).replace('R$ ', '');
}

/** Encurta rótulos longos de órgão/município para caber no eixo. */
export function truncateLabel(label: string, max = 28) {
  return label.length > max ? `${label.slice(0, max - 1)}…` : label;
}
