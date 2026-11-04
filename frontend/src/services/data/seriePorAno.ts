import type { CompositionRow, CompositionSeries, TransferRecord } from './transferData';

/**
 * Série por ano com uma categoria empilhada (situação, por exemplo).
 *
 * - Só entram registros com ano de 4 dígitos e categoria preenchida.
 * - Mantém as `topN` categorias mais frequentes; o restante vira "Outros".
 * - Retorna as linhas em ordem crescente de ano, prontas para o gráfico empilhado.
 */
export function serieEmpilhadaPorAno(
  rows: TransferRecord[],
  colunaAno: string,
  colunaCategoria: string,
  { topN = 6, aceitar }: { topN?: number; aceitar?: (row: TransferRecord) => boolean } = {},
): { rows: CompositionRow[]; series: CompositionSeries[] } {
  const base = rows.filter((row) => {
    const ano = String(row[colunaAno] ?? '').trim();
    const categoria = String(row[colunaCategoria] ?? '').trim();
    if (!/^\d{4}$/.test(ano) || !categoria || categoria.startsWith('Não se aplica')) return false;
    return aceitar ? aceitar(row) : true;
  });

  const contagem = new Map<string, number>();
  for (const row of base) {
    const categoria = String(row[colunaCategoria]).trim();
    contagem.set(categoria, (contagem.get(categoria) ?? 0) + 1);
  }
  const principais = [...contagem.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, topN)
    .map(([categoria]) => categoria);
  const chaveDe = (categoria: string) =>
    principais.includes(categoria) ? `s_${principais.indexOf(categoria)}` : 's_outros';

  const series: CompositionSeries[] = principais.map((categoria, index) => ({
    key: `s_${index}`,
    label: categoria,
  }));
  if (contagem.size > principais.length) series.push({ key: 's_outros', label: 'Outros' });

  const porAno = new Map<string, CompositionRow>();
  for (const row of base) {
    const ano = String(row[colunaAno]).trim();
    const linha =
      porAno.get(ano) ??
      ({ label: ano, total: 0, ...Object.fromEntries(series.map((s) => [s.key, 0])) } as CompositionRow);
    const chave = chaveDe(String(row[colunaCategoria]).trim());
    linha[chave] = (linha[chave] as number) + 1;
    linha.total += 1;
    porAno.set(ano, linha);
  }

  return {
    rows: [...porAno.values()].sort((a, b) => a.label.localeCompare(b.label)),
    series,
  };
}
