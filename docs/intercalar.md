# Revisão para a avaliação intercalar

Fonte de verdade: leitura integral de `cc-lei-2627-tp2-1.pdf`, em particular
§§3, 4, 5, 6, 7 e 8.1. Esta nota documenta a revisão técnica; não é o relatório
final LNCS. Fases 1/2 verificadas localmente; validação na topologia CORE pendente.

## A. Fase 1

Os três papéis arrancam pelo run.sh, inclusive noutra diretoria e com espaços no
caminho do projeto/configuração. Script executável, LF e exec do Python. Configuração
sem valores por omissão, IPv4 e portas externas; TCP delimita frames, WELCOME anuncia
UDP, SUBSCRIBE encaminha para o destinatário certo. Logs contêm Unix em ms, ID,
tópico e message_id; flush por linha. Os processos são não interativos, não produzem
stdout normal e respeitam duration/start_after/drain. Só usam biblioteca padrão.

Problemas corrigidos: --config relativo era aceite; campos escalares admitiam
termos adicionais em `log`; nomes aceitavam DEL/C1; tópicos demasiado longos podiam
passar a configuração e falhar apenas ao gerar a publicação. Validação semântica
de WELCOME foi centralizada no parser. Não foi necessário reescrever os papéis.

## B. Fase 2

O mecanismo existente já implementava G1 nos dois saltos, com pendentes por
transmissão e por subscriber, retransmissões seletivas de 250 ms e pipelining.
O subscriber G1 não consulta nem preenche a tabela de duplicados: regista cada
chegada. O broker não aceita UDP de cliente sem WELCOME enviado, e o publisher
espera WELCOME completo e start_after. Sessão e endpoint inválidos são rejeitados.

Evicção existente confirmada: começa a contar com o primeiro pendente, reinicia
apenas por ACK de entrega pendente, não expulsa inativos sem pendentes e fecha TCP
e limpa filas no prazo. Reforçou-se a invalidação explícita dos campos de estado
da sessão ao desconectar. Corrigiu-se também o BYE: no esgotamento do contador de
controlo já não reutiliza o último ID. Os demais contadores já recusavam overflow.

Não foi encontrado erro funcional remanescente nas Fases 1/2 nos cenários testados.
Isso não certifica funcionamento em todas as cargas nem substitui o CORE.
Mantiveram-se G2, fan-out, filtros sobrepostos e ações at existentes. `peer` é lido
e validado, mas ligações entre brokers não estão implementadas e são recusadas
explicitamente; pertencem à Fase 4, fora desta revisão.

## C. Alterações

| Ficheiros | Alteração / motivo |
| --- | --- |
| src/config.py | Validação de caminho absoluto e valores escalares |
| src/protocol.py | Limites de representação, nomes, WELCOME, tipo ao codificar e leituras negativas |
| src/client.py | Evitar reutilizar ID no BYE; usar validação centralizada de WELCOME |
| src/broker/broker.py | Invalidar explicitamente estado da sessão fechada |
| tests/test_review.py | Regressões de contrato, protocolo e temporizadores |
| tests/test_integration.py, tests/loss_runner.py | Sockets/processos reais, perdas e sinais |
| tests/test_artifacts.py | Configurações CORE, ZIP e comparação de logs |
| configs/core/*.conf | Exemplos oficiais para broker1/pub1/sub1, G1 |
| tools/check_logs.py | Conferir conjuntos e duplicados de publicações/entregas |
| tools/make_submission.py | Arquivo com nomes reais fornecidos pelo utilizador |
| README.md, docs/*.md | Instruções, protocolo atualizado, evidências e guião CORE |

run.sh, main.py, publisher.py e subscriber.py foram revistos e os seus comportamentos
testados; não precisaram de alterações. Nenhum push foi efetuado.

## D. Evidência dos testes

Ambiente: Linux, Python 3.12.3, sockets localhost. Comando:

```sh
python3 -m unittest discover -s tests -v
```

Suite original: 14 testes aprovados após permitir sockets fora do sandbox.
Suite ampliada: **49 testes aprovados, 0 falhados, 0 ignorados**, incluindo uma
execução adicional do teste de rede dentro de um ZIP temporário extraído. O teste
do pacote lança os três papéis com `python3 -S`, sem carregar site-packages.
Todos os imports da aplicação são também verificados como locais/biblioteca padrão.
Não há instalação de dependências nem chamadas a serviços na Internet.

| Grupo de testes | Evidência |
| --- | --- |
| test_clients.py (13) | Fluxos, at, G1/G2, flush antes de ACK, perdas simuladas nos handlers |
| test_review.py (21) | Chaves/valores inválidos, bytes/endianness, fragmentação/coalescência, truncamentos, overflow, estado/evicção |
| test_network.py (1) | Três papéis reais noutra diretoria; portas efémeras, fan-out, seletividade e timestamps |
| test_integration.py (11) | Perdas reais induzidas antes de sendto, sinais, prazos, sessão, erros e pacote extraído |
| test_artifacts.py (3) | Exemplos CORE, estrutura/permissão ZIP e comparador |

O ensaio de perda real gera 80 publicações G1 a 500/s. Suprime a primeira tentativa
de PUBLISH tx=1, DELIVER tx=1, ACK do broker tx=2 e ACK do subscriber tx=3. Verifica
80 IDs únicos entregues, mais de 80 linhas de entrega, exatamente 80 linhas no
publisher e envio de tx=20 antes da retransmissão de tx=1. Não é perda aleatória
no kernel: os processos descartam tentativas selecionadas num launcher de teste;
as restantes mensagens atravessam sockets TCP/UDP reais.

O ensaio de drain perde todos os ACKs do broker: observa retransmissões e saída ao
fim de 0,75 s após a publicação. O ensaio de expulsão observa EOF no TCP próximo
de 0,6 s com pendentes, depois de confirmar que 0,8 s de inatividade sem pendentes
não expulsam. SIGSTOP/SIGCONT testados nos grupos dos processos: suspensão de
0,45 s com evict_after=1,2 s recupera todas as publicações; suspensão de 1,6 s
provoca saída ao retomar e não entrega publicação feita após o prazo.

O sandbox bloqueava sockets; a execução inicial ignorou o teste de rede. Essa
execução não foi tomada como prova de TCP/UDP. Os resultados acima são da execução
fora do sandbox, onde não houve testes ignorados. CORE não executado: não há CORE
nem VM neste ambiente. tcpdump está instalado; não foram produzidas capturas
locais nem CORE nesta revisão, e os comandos para as capturas da demonstração
ficam em core.md.

## E. Trabalho manual restante

1. Construir/guardar a sessão CORE oficial com os quatro routers em anel, cinco
   LAN e OSPF; confirmar encaminhamento e Python 3.10+ na VM.
2. Executar baseline, perdas nas duas direções de L1 e suspensões curta/longa,
   seguindo [core.md](core.md); guardar configurações, logs e capturas.
3. Abrir uma captura junto de [protocol.md](protocol.md) e praticar a explicação
   de WELCOME, session_id, transmission_id, message_id, ACK e retransmissões.
4. Gerar o ZIP com turno e grupo reais; extrair e executar na VM sem instalar nada.
   O ZIP temporário dos testes usa TEST apenas como fixture e é eliminado.
5. Entregar na plataforma até às 23h59 do dia da avaliação intercalar do turno.

## F. Preparação e riscos

Mostrar uma publicação, depois carga G1 com perdas e duplicados, depois recuperação
e expulsão por suspensão. Explicar por que existem dois IDs, por que uma chegada
G1 duplicada produz outra linha, o que significa o ACK de cada salto e por que
start_after conta da sessão enquanto at conta do arranque. Estudar protocol.py,
NetworkClient._control_message, Publisher._tick, Subscriber._datagram e
Broker._publish/_receive_datagrams/_timers/_disconnect.

Limitações a saber justificar: RTO fixo de 250 ms, timeout inicial/control de 10 s,
pendentes sem janela máxima (memória cresce com carga/interrupção), ausência de
controlo de congestionamento e de recuperação persistente/reconexão após expulsão.
Perdas permanentes ou budgets de execução insuficientes impossibilitam garantir
todas as entregas. Estes limites são documentados, não apresentados como testes
CORE aprovados. A carga local não prova capacidade ilimitada.

## Checklist obrigatória das Fases 1/2

✅ significa implementado e testado localmente. 🟡 significa preparado/implementado
mas por validar na VM/CORE. Não há ❌ identificado nas Fases 1/2 após esta revisão.

| Requisito | Estado |
| --- | --- |
| run.sh dos três papéis, qualquer diretoria, --id e --config | ✅ |
| Permissão executável, LF, sem instalações externas | ✅ |
| Configuração: obrigatórias/repetíveis, comentários, opções, formatos, sem defaults | ✅ |
| Caminhos absolutos, IPv4 e portas configuráveis | ✅ |
| SubControl TCP com delimitação; campos binários diretos | ✅ |
| Sessão e descoberta da porta UDP através de WELCOME | ✅ |
| SUBSCRIBE, PUBLISH e encaminhamento EventCast real | ✅ |
| Logs com quatro campos, Unix ms, mesmo message_id e flush | ✅ |
| duration, start_after, drain, fecho autónomo e SIGTERM | ✅ |
| Processos sem UI/stdout normal, no grupo de lançamento | ✅ |
| WELCOME completo antes do EventCast; sessão/origem inválidas rejeitadas | ✅ |
| IDs de sessão/publicação/transmissão, associação de ACK e overflow | ✅ |
| G1 com perdas de PUBLISH, DELIVER e ACK nos dois saltos | ✅ |
| Retransmissão seletiva, temporizadores e várias publicações pendentes | ✅ |
| Repetições sem novas linhas no publisher; duplicados registados em G1 | ✅ |
| Evicção só com pendentes; ACK válido renova prazo | ✅ |
| Suspensão curta recupera; longa fecha TCP e limpa fila | ✅ |
| Expulso não recebe publicações posteriores; inativo não é expulso | ✅ |
| Truncamentos, comprimentos, ordem dos bytes e documentação coerente | ✅ |
| Estrutura do ZIP e execução do arquivo extraído | ✅ |
| Topologia oficial/OSPF, conectividade e configuração da VM | 🟡 |
| Repetição de todos os ensaios de rede no CORE | 🟡 |
| Capturas reais e demonstração perante o docente | 🟡 |

O nome definitivo do ZIP depende dos valores reais de turno/grupo ainda não
fornecidos; não se inventaram identificadores nem se preparou relatorio.pdf.
