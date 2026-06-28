/**
 * Sincroniza os CSVs gerados pelo backend para public/data/
 *
 * Executar após rodar: python backend/api.py
 * Uso: yarn sync-data
 */

import { copyFileSync, existsSync, mkdirSync, writeFileSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const PUBLIC_DATA = resolve(ROOT, 'public', 'data');

mkdirSync(PUBLIC_DATA, { recursive: true });

const mappings = [
  { src: resolve(ROOT, 'dataset', 'emendas_to.csv'),      dst: resolve(PUBLIC_DATA, 'especiais.csv') },
  { src: resolve(ROOT, 'dataset', 'fundo_a_fundo.csv'),   dst: resolve(PUBLIC_DATA, 'fundo-a-fundo.csv') },
];

let ok = 0;
for (const { src, dst } of mappings) {
  if (!existsSync(src)) {
    console.warn(`[sync] Arquivo não encontrado, ignorando: ${src}`);
    continue;
  }
  copyFileSync(src, dst);
  console.log(`[sync] ${src} → ${dst}`);
  ok++;
}

if (ok === 0) {
  console.error('\n[sync] Nenhum arquivo copiado. Execute python backend/api.py primeiro.\n');
  process.exit(1);
} else {
  const now = new Date().toISOString();
  const manifest = {
    especiais: now,
    'fundo-a-fundo': now,
    'discricionarias-legais': now,
  };
  writeFileSync(resolve(PUBLIC_DATA, 'manifest.json'), JSON.stringify(manifest, null, 2), 'utf-8');
  console.log('[sync] manifest.json gerado.');
  console.log(`\n[sync] ${ok} arquivo(s) copiado(s) com sucesso.\n`);
}
