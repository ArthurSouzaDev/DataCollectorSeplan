import { Link } from 'react-router-dom';

const GUIA_COMPLETO =
  'https://github.com/ArthurSouzaDev/DataCollectorSeplan/blob/main/Doc/GUIA_DO_USUARIO.md';

export function AjudaPage() {
  return (
    <section className="page detail-page">
      <nav className="breadcrumb" aria-label="Navegação">
        <Link className="back-link" to="/">Início</Link>
        <span className="breadcrumb-sep" aria-hidden="true">›</span>
        <span className="breadcrumb-current">Ajuda</span>
      </nav>

      <div className="detail-header">
        <div>
          <h1>Ajuda</h1>
          <p>Resumo de como usar o painel. O guia completo traz todas as fontes, filtros e indicadores.</p>
          <a className="source-label" href={GUIA_COMPLETO} target="_blank" rel="noreferrer">
            Abrir o guia completo (Markdown)
          </a>
        </div>
      </div>

      <section className="panel help-section">
        <h2>Fontes de dados</h2>
        <ul>
          <li>
            <strong>Fundo a fundo:</strong> planos de transferências fundo a fundo do Tocantins. Fonte: API do
            Transferegov. Atualiza a cada 12 horas.
          </li>
          <li>
            <strong>Especiais:</strong> emendas parlamentares especiais para beneficiários do Tocantins. Fonte: API
            do Transferegov. Atualiza a cada 12 horas.
          </li>
          <li>
            <strong>Especiais completo:</strong> os 24 conjuntos de dados do módulo de especiais, do Tocantins. Fonte:
            API do Transferegov. Atualiza diariamente.
          </li>
          <li>
            <strong>Discricionárias e legais:</strong> propostas e convênios do Tocantins. Fonte: API do Transferegov.
            Atualiza diariamente.
          </li>
        </ul>
      </section>

      <section className="panel help-section">
        <h2>Como usar os filtros</h2>
        <ul>
          <li>
            Os filtros ficam no topo de cada painel e atualizam KPIs, gráficos e tabela juntos.
          </li>
          <li>
            <strong>Sem seleção = todos.</strong> Para remover um item, clique no <code>x</code> do chip.
          </li>
          <li>
            Clicar numa barra do ranking ou numa fatia do donut aplica o filtro daquele item. <em>Limpar seleção</em> desfaz.
          </li>
          <li>
            <strong>Limpar filtros</strong> zera tudo, inclusive a busca.
          </li>
          <li>
            <strong>Baixar CSV</strong> exporta só os registros filtrados, com as colunas visíveis.
          </li>
          <li>
            Na base de discricionárias, o filtro de Anos usa o ano de assinatura do convênio. Propostas sem convênio
            somem quando um ano é selecionado.
          </li>
        </ul>
      </section>

      <section className="panel help-section">
        <h2>Módulo completo de transferências especiais</h2>
        <p>
          Acessado pelo menu em <Link to="/transf-especiais">Especiais completo</Link>. Reúne os 24 conjuntos de dados
          da API de especiais do Tocantins, organizados em grupos: planos de ação, execução orçamentária, gestão
          financeira, executores e metas, planos de trabalho e análises, e relatórios de gestão.
        </p>
        <ul>
          <li>
            <strong>Indicadores no topo:</strong> planos, valores, empenhado, documentos hábeis, lançamentos e saldo
            atual das contas. O saldo usa o registro mais recente de cada conta.
          </li>
          <li>
            <strong>Gráficos clicáveis:</strong> clique numa barra, fatia ou coluna por ano para filtrar a planilha.
            Clique de novo no mesmo item para desfazer. A página não rola até a planilha.
          </li>
          <li>
            <strong>Donuts</strong> mostram quantidade de registros, não valor. A fatia "Outros" agrupa as categorias
            menores e não filtra.
          </li>
          <li>
            <strong>Planilha no fim da página:</strong> escolha o conjunto de dados, use os filtros de múltipla escolha,
            a busca e as colunas visíveis. A tabela mostra as primeiras 150 linhas. O <code>Baixar CSV</code> traz todos
            os registros filtrados.
          </li>
          <li>
            <strong>Os gráficos não são filtrados pela planilha.</strong> Só os cliques nos gráficos preenchem os
            filtros da planilha.
          </li>
          <li>
            <strong>Atualização:</strong> diária, às 03:30. A data da última coleta aparece no topo da página. Se um
            conjunto não tiver registros, o gráfico ou a tabela mostram a mensagem "Sem dados".
          </li>
        </ul>
      </section>

      <section className="panel help-section">
        <h2>Como ler os indicadores</h2>
        <ul>
          <li>
            <strong>Discricionárias:</strong> o valor global é repasse + contrapartida. O valor liberado vem do Portal da
            Transparência para os convênios encontrados. Pago, saldo em conta e valores devolvidos aparecem como
            "Indisponível", porque nenhuma fonte atual traz esses dados.
          </li>
          <li>
            <strong>Percentuais de aprovadas e de assinatura:</strong> o denominador exclui as propostas ainda
            cadastradas.
          </li>
        </ul>
      </section>

      <section className="panel help-section">
        <h2>Histórico de atualizações</h2>
        <ul>
          <li>
            A tela <Link to="/historico">Histórico</Link> mostra o que mudou em cada coleta: registros novos e
            removidos, campos alterados e variação de totais. Guarda os últimos 90 dias.
          </li>
        </ul>
      </section>

      <section className="panel help-section">
        <h2>Dúvidas comuns</h2>
        <ul>
          <li>
            <strong>Onde fica "Em execução"?</strong> Na coluna <code>situacao_convenio</code>, vinda do Portal. O filtro
            de situações e o donut usam o grupo da proposta, como no painel da SERPRO.
          </li>
          <li>
            <strong>Por que valor pago, saldo e devolvidos estão "Indisponível"?</strong> Nenhuma fonte atual traz
            esses dados. Mostrar zero seria enganoso, então o painel não mostra um valor.
          </li>
          <li>
            <strong>Por que o total não bate com o painel da SERPRO?</strong> A base usa o número de convênio das metas
            de planos de trabalho. Convênios fora dessa fonte não são contados.
          </li>
        </ul>
      </section>
    </section>
  );
}
