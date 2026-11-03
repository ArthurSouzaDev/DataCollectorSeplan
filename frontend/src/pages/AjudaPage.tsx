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
        <h2>Como ler os indicadores</h2>
        <ul>
          <li>
            <strong>Discricionárias:</strong> o valor global é repasse + contrapartida. Desembolso, pago, saldo em conta
            e valores devolvidos aparecem zerados porque a API atual não os fornece.
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
            <strong>Por que "Em execução" não aparece?</strong> É uma situação de convênio, que a fonte atual de
            discricionárias não traz. Ela volta com a integração do Portal da Transparência.
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
