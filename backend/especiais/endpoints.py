"""Registro dos endpoints do módulo de Transferências Especiais.

Cada endpoint pode ser:
- raiz: consultado uma vez (com `filtros` fixos, se houver);
- dependente: consultado uma vez por valor distinto de uma coluna de outro dataset
  já coletado (`dependencia`). Ex.: planos de ação são buscados por id_beneficiario.

A lista está em ordem de dependência: um dataset só aparece depois dos que ele usa.
Para incluir um endpoint novo, basta acrescentar um `Endpoint` aqui.

Documentação da API: https://api-publica.transferegov.gestao.gov.br/especiais/docs
"""

from __future__ import annotations

from dataclasses import dataclass, field

UF_RECORTE = "TO"


@dataclass(frozen=True)
class Dependencia:
    """Consulta o endpoint once por valor de `coluna_origem` do dataset `origem`."""

    origem: str          # nome de outro Endpoint já coletado
    coluna_origem: str   # coluna do dataset de origem que contém os valores
    parametro: str       # parâmetro de query da API que recebe cada valor


@dataclass(frozen=True)
class Endpoint:
    nome: str                       # nome do dataset e do arquivo CSV gerado
    caminho: str                    # caminho relativo à base da API
    descricao: str
    filtros: dict = field(default_factory=dict)   # filtros fixos enviados em toda consulta
    dependencia: Dependencia | None = None
    paginado: bool = True           # False para endpoints que devolvem um objeto único


ENDPOINTS: list[Endpoint] = [
    # ── Atualização e referências ─────────────────────────────────────────
    Endpoint(
        nome="data_atualizacao",
        caminho="/data-atualizacao",
        descricao="Data da última atualização da base",
        paginado=False,
    ),
    Endpoint(
        nome="programas",
        caminho="/programas-especiais",
        descricao="Programas especiais",
    ),

    # ── Beneficiários e planos de ação (eixo principal) ───────────────────
    Endpoint(
        nome="beneficiarios",
        caminho="/beneficiarios-especiais",
        descricao="Beneficiários (entes) do Tocantins",
        filtros={"uf_beneficiario": UF_RECORTE},
    ),
    Endpoint(
        nome="planos_acao",
        caminho="/planos-acao-especiais",
        descricao="Planos de ação, por beneficiário",
        dependencia=Dependencia("beneficiarios", "id_beneficiario", "id_beneficiario"),
    ),
    Endpoint(
        nome="planos_acao_historico",
        caminho="/planos-acao-historico-especiais",
        descricao="Histórico de situação dos planos de ação",
        dependencia=Dependencia("planos_acao", "id_plano_acao", "id_plano_acao"),
    ),
    Endpoint(
        nome="executores",
        caminho="/executores-especiais",
        descricao="Executores dos planos de ação",
        dependencia=Dependencia("planos_acao", "id_plano_acao", "id_plano_acao"),
    ),
    Endpoint(
        nome="meta",
        caminho="/meta-especiais",
        descricao="Metas dos executores",
        dependencia=Dependencia("executores", "id_executor", "id_executor"),
    ),
    Endpoint(
        nome="finalidade",
        caminho="/finalidade-especiais",
        descricao="Áreas de política pública (finalidade) dos executores",
        dependencia=Dependencia("executores", "id_executor", "id_executor"),
    ),
    Endpoint(
        nome="devolucao",
        caminho="/devolucao-especiais",
        descricao="Devoluções de recursos",
        dependencia=Dependencia("planos_acao", "id_plano_acao", "id_plano_acao"),
    ),

    # ── Planos de trabalho e análises ─────────────────────────────────────
    Endpoint(
        nome="planos_trabalho",
        caminho="/planos-trabalho-especiais",
        descricao="Planos de trabalho",
        dependencia=Dependencia("planos_acao", "id_plano_acao", "id_plano_acao"),
    ),
    Endpoint(
        nome="planos_trabalho_historico",
        caminho="/planos-trabalho-historico",
        descricao="Histórico de situação dos planos de trabalho",
        dependencia=Dependencia("planos_trabalho", "id_plano_trabalho", "id_plano_trabalho"),
    ),
    Endpoint(
        nome="planos_trabalho_analises",
        caminho="/planos-trabalho-analises-especiais",
        descricao="Análises dos planos de trabalho",
        dependencia=Dependencia("planos_trabalho", "id_plano_trabalho", "id_plano_trabalho"),
    ),
    # Consulta raiz: o id_plano_trabalho_analise_pt do histórico não coincide com nenhum
    # id das análises (interseção zero na base nacional), então não dá para encadeá-la.
    Endpoint(
        nome="planos_trabalho_analise_historico",
        caminho="/plano-trabalho-analise-historico-especiais",
        descricao="Histórico das análises dos planos de trabalho (base nacional)",
    ),
    Endpoint(
        nome="orgaos_analises_pendentes",
        caminho="/orgaos-analises-pendentes-especiais",
        descricao="Órgãos com análises pendentes nos planos de trabalho",
        dependencia=Dependencia("planos_trabalho", "id_plano_trabalho", "id_plano_trabalho"),
    ),

    # ── Relatórios de gestão ──────────────────────────────────────────────
    Endpoint(
        nome="relatorios_gestao",
        caminho="/relatorios-gestao-especiais",
        descricao="Relatórios de gestão",
        dependencia=Dependencia("planos_acao", "id_plano_acao", "id_plano_acao"),
    ),
    Endpoint(
        nome="relatorios_gestao_novos",
        caminho="/relatorios-gestao-novos-especiais",
        descricao="Relatórios de gestão (novo modelo)",
        dependencia=Dependencia("planos_acao", "id_plano_acao", "id_plano_acao"),
    ),
    Endpoint(
        nome="relatorios_gestao_analise",
        caminho="/relatorios-gestao-analise-especiais",
        descricao="Análises dos relatórios de gestão",
        dependencia=Dependencia(
            "relatorios_gestao_novos", "id_relatorio_gestao_novo", "id_relatorio_gestao_novo"
        ),
    ),
    Endpoint(
        nome="relatorios_gestao_documento_liquidacao",
        caminho="/relatorios-gestao-documento-liquidacao-especiais",
        descricao="Documentos de liquidação dos relatórios de gestão",
        dependencia=Dependencia(
            "relatorios_gestao_novos", "id_relatorio_gestao_novo", "id_relatorio_gestao_novo"
        ),
    ),

    # ── Execução orçamentária e financeira ────────────────────────────────
    Endpoint(
        nome="empenhos",
        caminho="/empenhos-especiais",
        descricao="Empenhos",
        dependencia=Dependencia("planos_acao", "id_plano_acao", "id_plano_acao"),
    ),
    Endpoint(
        nome="documentos_habeis",
        caminho="/documentos-habeis-especiais",
        descricao="Documentos hábeis (DH)",
        dependencia=Dependencia("empenhos", "id_empenho", "id_empenho"),
    ),
    Endpoint(
        nome="ordens_pagamento",
        caminho="/ordens-pagamentos-ordens-bancarias-especiais",
        descricao="Ordens de pagamento e ordens bancárias",
        dependencia=Dependencia("documentos_habeis", "id_dh", "id_dh"),
    ),
    Endpoint(
        nome="gestao_financeira_lancamentos",
        caminho="/gestao-financeira-lancamentos-especiais",
        descricao="Lançamentos da gestão financeira (conta específica)",
        dependencia=Dependencia("planos_acao", "id_agencia_conta", "id_agencia_conta"),
    ),
    # Consulta raiz: a base nacional tem só ~14 mil subtransações e nenhum dos lançamentos
    # do TO testados aparece nela (interseção zero), então buscar tudo é mais simples e completo.
    Endpoint(
        nome="gestao_financeira_subtransacoes",
        caminho="/gestao-financeira-subtransacoes-especiais",
        descricao="Subtransações dos lançamentos financeiros (base nacional)",
    ),
    Endpoint(
        nome="saldo_conta",
        caminho="/saldo-conta-gestao-financeira-especiais",
        descricao="Saldos das contas da gestão financeira",
        dependencia=Dependencia("planos_acao", "id_agencia_conta", "id_agencia_conta"),
    ),
]
