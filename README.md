# TP2 — Sistema de Vigilância e Alerta Ambiental

Implementação em Python 3.10+ e biblioteca padrão, sem instalação de pacotes nem
acesso à Internet. SubControl/TCP estabelece sessões e subscrições;
EventCast/UDP transporta publicações, entregas e confirmações.

Na raiz do projeto, em três terminais:

```sh
./run.sh broker --id broker1 --config "$PWD/configs/broker.conf"
./run.sh subscriber --id sub1 --config "$PWD/configs/subscriber.conf"
./run.sh publisher --id pub1 --config "$PWD/configs/publisher.conf"
```

O script também pode ser invocado por caminho absoluto a partir de outra diretoria.
`--config` e `log` são caminhos absolutos. A diretoria dos logs tem de existir.
Cada execução substitui os logs anteriores; guardar os resultados entre ensaios.

```sh
python3 -m unittest discover -s tests -v
```

Os testes de rede precisam de permissão para abrir sockets locais; incluem perdas
determinísticas e SIGSTOP/SIGCONT em processos reais. São ensaios localhost, não CORE.

- [Revisão intercalar e checklist](docs/intercalar.md)
- [Topologia e demonstração no CORE](docs/core.md)
- [Campos binários, trocas e temporizadores](docs/protocol.md)
- [Funcionamento dos clientes](docs/clients.md)

Para preparar a entrega, fornecer os valores reais do turno e grupo:

```sh
python3 tools/make_submission.py --turno "$TURNO" --grupo "$GRUPO" --output-dir /tmp
```

O arquivo `CC-TP2-PL<Turno>-G<Grupo>.zip` inclui `run.sh` na raiz, `src`,
configurações, documentação, ferramentas e testes. Não inclui Git, caches nem logs.
O script conserva a permissão executável no ZIP. O relatório PDF só é exigido na
entrega final. Não foi gerado um arquivo de entrega com turno/grupo inventados.

As ligações entre brokers (`peer`) continuam reservadas à Fase 4: a configuração
é lida e validada, mas uma lista não vazia é recusada explicitamente pelo broker.
O comportamento G2 e as ações `at` já existentes foram preservados nesta revisão.
