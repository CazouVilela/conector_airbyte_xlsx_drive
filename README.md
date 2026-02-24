# Source Google Sheets XLSX

Conector Airbyte que lê tanto **Google Sheets nativos** quanto **arquivos XLSX** armazenados no Google Drive.

## Problema

O conector oficial do Airbyte para Google Sheets **rejeita** arquivos XLSX importados no Google Drive (MIME type `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet`), retornando `HttpError 400: "This operation is not supported for this document"`.

## Solucao

Conector dual-mode que detecta o tipo do arquivo via Drive API e roteia:
- **XLSX no Drive** -> Download via Drive API + parse com `openpyxl`
- **Google Sheets nativo** -> Sheets API v4 `spreadsheets.values.get()`

Cada aba/sheet vira um stream separado. Full Refresh only.

## Instalacao

```bash
pip install -e .
```

## Configuracao

Crie `secrets/config.json`:

```json
{
  "credentials_json": "<conteudo do JSON da service account>",
  "spreadsheet_id": "<ID do arquivo no Google Drive>",
  "names_conversion": true
}
```

**Credenciais**: Service account com acesso de leitura ao arquivo no Google Drive.

## Uso

```bash
# Spec
python main.py spec

# Testar conexao
python main.py check --config secrets/config.json

# Descobrir streams
python main.py discover --config secrets/config.json

# Ler dados
python main.py read --config secrets/config.json --catalog configured_catalog.json
```

## Testes

```bash
pip install -e ".[tests]"
pytest -v
```

## Docker

```bash
docker build -t airbyte/source-google-sheets-xlsx:1.0.0 .
```

## Documentacao

Documentacao detalhada em [`/documentacao`](./documentacao).
