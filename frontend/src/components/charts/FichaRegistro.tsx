import { useEffect } from 'react';

import { formatCurrency, toNumber, type TransferRecord } from '../../services/data/transferData';

/**
 * Campos que não aparecem na ficha: dados bancários e de contato (pessoais ou sem utilidade
 * para leitura) e as chaves que já formam o título.
 */
const CAMPOS_OCULTOS = new Set([
  'banco', 'agencia', 'conta', 'dv_agencia', 'dv_conta',
  'cd_agencia', 'cd_conta', 'cd_banco', 'nm_banco',
  'email_proponente', 'telefone_proponente', 'fax_proponente',
  'endereco_proponente', 'cep_proponente', 'bairro_proponente', 'email_camara',
  'codigo_plano', 'nr_convenio', 'id_proposta',
]);
/** Valores que ganham cartão no topo da ficha. */
const CARTOES_VALOR = ['valor_total', 'valor_custeio', 'valor_investimento'];

/** Posição da linha clicada, em coordenadas da janela. */
export type Ancora = { x: number; y: number; topo: number };

type FichaRegistroProps = {
  row: TransferRecord;
  titulo: string;
  situacaoColumn: string;
  ancora: Ancora;
  onClose: () => void;
};

const LARGURA_FICHA = 520;
const MARGEM = 12;

function rotulo(coluna: string) {
  const texto = coluna.replaceAll('_', ' ');
  return texto.charAt(0).toUpperCase() + texto.slice(1);
}

function formatarValor(coluna: string, valor: string | number) {
  if (coluna.startsWith('valor_') || coluna.startsWith('vl_')) return formatCurrency(toNumber(valor));
  const texto = String(valor ?? '').trim();
  return texto || 'Não informado';
}

/**
 * Ficha completa de um registro, aberta ao clicar numa linha da planilha.
 * Mostra só os campos que a base já tem; não busca outra fonte.
 */
export function FichaRegistro({ row, titulo, situacaoColumn, ancora, onClose }: FichaRegistroProps) {
  useEffect(() => {
    function fechar(evento: KeyboardEvent) {
      if (evento.key === 'Escape') onClose();
    }
    document.addEventListener('keydown', fechar);
    return () => document.removeEventListener('keydown', fechar);
  }, [onClose]);

  const situacao = String(row[situacaoColumn] ?? '').trim() || 'Não informado';
  const cartoes = CARTOES_VALOR.filter((campo) => campo in row);
  const campos = Object.keys(row).filter(
    (campo) =>
      !CAMPOS_OCULTOS.has(campo) &&
      !CARTOES_VALOR.includes(campo) &&
      campo !== situacaoColumn,
  );

  // Largura limitada à tela e posição junto à linha: abaixo dela, ou acima se estiver na metade de baixo.
  const largura = Math.min(LARGURA_FICHA, window.innerWidth - MARGEM * 2);
  const left = Math.max(MARGEM, Math.min(ancora.x, window.innerWidth - largura - MARGEM));
  const posicaoVertical =
    ancora.y > window.innerHeight * 0.5
      ? { bottom: window.innerHeight - ancora.topo + 8 }
      : { top: ancora.y + 8 };

  return (
    <div className="ficha-overlay" onClick={onClose}>
      <div
        className="ficha-card"
        style={{ left, width: largura, ...posicaoVertical }}
        role="dialog"
        aria-modal="false"
        aria-labelledby="ficha-titulo"
        onClick={(evento) => evento.stopPropagation()}
      >
        <header className="ficha-header">
          <div>
            <span className="ficha-kicker">Ficha completa</span>
            <h2 id="ficha-titulo">{titulo}</h2>
          </div>
          <div className="ficha-header-actions">
            <span className="ficha-status">{situacao}</span>
            <button className="ficha-close" type="button" onClick={onClose} aria-label="Fechar ficha">
              ×
            </button>
          </div>
        </header>

        {cartoes.length ? (
          <div className="ficha-cartoes">
            {cartoes.map((campo) => (
              <div key={campo} className="ficha-cartao">
                <span>{rotulo(campo)}</span>
                <strong>{formatarValor(campo, row[campo])}</strong>
              </div>
            ))}
          </div>
        ) : null}

        <dl className="ficha-campos">
          {campos.map((campo) => (
            <div key={campo} className="ficha-campo">
              <dt>{rotulo(campo)}</dt>
              <dd>{formatarValor(campo, row[campo])}</dd>
            </div>
          ))}
        </dl>
      </div>
    </div>
  );
}
