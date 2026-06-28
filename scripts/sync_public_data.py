import json
import datetime
import pathlib
import shutil

import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parent.parent
PUBLIC_DATA = ROOT / "public" / "data"
PUBLIC_DATA.mkdir(parents=True, exist_ok=True)

shutil.copy(ROOT / "dataset" / "emendas_to.csv",    PUBLIC_DATA / "especiais.csv")
shutil.copy(ROOT / "dataset" / "fundo_a_fundo.csv", PUBLIC_DATA / "fundo-a-fundo.csv")

df = pd.read_parquet(ROOT / "data_discricionarias" / "processados" / "discricionarias_to.parquet")
df.to_csv(PUBLIC_DATA / "discricionarias-legais.csv", sep=";", index=False, encoding="utf-8-sig")

now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=-3))).isoformat()
manifest = {
    "especiais": now,
    "fundo-a-fundo": now,
    "discricionarias-legais": now,
}
(PUBLIC_DATA / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
print(f"Sincronizado. manifest.json gerado em {now}")
