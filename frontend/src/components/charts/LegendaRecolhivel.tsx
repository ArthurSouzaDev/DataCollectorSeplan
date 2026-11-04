import type { ReactNode } from 'react';

type LegendaRecolhivelProps = {
  /** Quantidade de itens da legenda, mostrada no cabeçalho. */
  quantidade: number;
  children: ReactNode;
};

/** Até este número de itens a legenda fica sempre visível; acima dele, recolhe. */
export const LIMITE_LEGENDA_FIXA = 4;

/**
 * Legenda que recolhe quando tem muitos itens. Com poucos itens, fica aberta e normal.
 * O cliente abre quando quiser ver os itens e filtrar por eles.
 */
export function LegendaRecolhivel({ quantidade, children }: LegendaRecolhivelProps) {
  if (quantidade <= LIMITE_LEGENDA_FIXA) return <>{children}</>;

  return (
    <details className="legenda-recolhivel">
      <summary>
        Legenda <span className="collapsible-count">{quantidade}</span>
      </summary>
      {children}
    </details>
  );
}
