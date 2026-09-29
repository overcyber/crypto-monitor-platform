# Changelog

## 4.3.6

- Corrige `ILLEGAL_AGGREGATION` no histórico da Web UI causado por colisão entre nomes de colunas e aliases agregados (`open`/`close`).
- Histórico passa a agregar sobre colunas renomeadas em subquery (`src_open`, `src_close`, `src_open_time`).
- Remove `FINAL` das consultas de cotação e limita a janela hot a 6 horas para reduzir latência/`ReadTimeout`.
- Timeout interno da Web UI aumentado para 15 s.


## 4.3.5

- adiciona período histórico selecionável: 1/3/7/14/30/60/90 dias;
- histórico da Web UI passa a usar `candles_1m` agregado diretamente no ClickHouse, com fallback para `events_hot`;
- gráfico usa timestamps reais no eixo X e cache separado por venue/ativo/período;
- datas do gráfico são exibidas em `America/Sao_Paulo`;
- documenta que erros `chrome-extension://` pertencem a extensões do navegador.

## 4.3.4

- Corrige definitivamente concorrência do ClickHouse: nenhum cliente/sessão é compartilhado entre threads/processos.
- `rows`, `command` e `insert` passam a abrir/fechar cliente dedicado por operação.
- Corrige histórico intermitente e cotações vazias causadas por 503 concorrente.
- Corrige Candle Worker e Reconciler para usar `insert()` isolado.
- Mantém o último gráfico válido apenas para falhas externas reais, não por sessão compartilhada.


- Web UI: Ativo agora é um seletor preenchido pela watchlist/cotações disponíveis.
- Web UI: gráfico de Histórico recente foi estabilizado e preserva o último histórico válido em falhas transitórias.
- Web UI: refresh rápido de mercado/analytics separado dos ciclos de pipeline e alertas para evitar sobreposição e respostas antigas.
- Web UI: consultas ClickHouse usam cliente/sessão independente por query; remove erro `Attempt to execute concurrent queries within the same session` em Inventário, Armazenamento, Candles e Microestrutura.
- Web UI: cache pesado protegido contra recomputação concorrente.
- Nota: erros `chrome-extension://...` vêm de extensões instaladas no navegador e não são gerados pela Web UI.

# Changelog

## 4.3.2

- corrige `ProgrammingError: Attempt to execute concurrent queries within the same session` na Web UI;
- serializa consultas ClickHouse feitas pela interface;
- torna `config/` gravável somente no container `web-ui`;
- transforma a aba Configuração em editor funcional de `markets.yaml`, `alerts.yaml` e `telegram.yaml`;
- adiciona validação server-side e backups automáticos antes de salvar;
- permite editar watchlist, exchanges, quotes, produtos, canais e parâmetros de depth;
- permite criar/remover/editar regras de alerta market/microstructure;
- permite configurar alertas Telegram por percentual, teto e piso;
- mantém token/chat IDs do Telegram fora da interface;
- converte timestamps operacionais da distribuição de dados para `America/Sao_Paulo`.

## 4.3.1

- corrige scripts Telegram e melhora cobertura do regime na interface.
