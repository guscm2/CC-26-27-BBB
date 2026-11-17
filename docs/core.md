# Preparação e demonstração CORE — Fases 1 e 2

Estado: procedimento preparado, **não executado no CORE**. O ambiente da revisão
não dispõe de CORE/VM. tcpdump está instalado, mas não foram produzidas capturas
nesta revisão; os testes automáticos usam localhost.
Construir e guardar a sessão na versão de CORE instalada na VM; não se fornece um
ficheiro de sessão dependente de uma versão que não foi possível verificar.

## 1. Construir a topologia oficial

Criar 4 nós router (r1–r4), 5 switches (sw1–sw5), 3 PC publishers,
6 PC subscribers e 3 PC brokers. Usar os serviços por omissão dos routers,
incluindo OSPF. Nas Fases 1/2 só broker1 executa a aplicação; manter toda a topologia.

| Switch / ligação | Nós e endereços /24 |
| --- | --- |
| sw1 ↔ r1 | r1=10.0.1.1; pub1–3=10.0.1.10–12 |
| sw2 ↔ r1 | r1=10.0.2.1; sub1–6=10.0.2.31–36 |
| sw3 ↔ r2 | r2=10.0.3.1; broker1=10.0.3.20 |
| sw4 ↔ r3 | r3=10.0.4.1; broker2=10.0.4.20 |
| sw5 ↔ r4 | r4=10.0.5.1; broker3=10.0.5.20 |
| L1: r1 ↔ r2 | r1=10.0.11.1; r2=10.0.11.2 |
| L2: r2 ↔ r3 | r2=10.0.12.1; r3=10.0.12.2 |
| L3: r3 ↔ r4 | r3=10.0.13.1; r4=10.0.13.2 |
| L4: r4 ↔ r1 | r4=10.0.14.1; r1=10.0.14.2 |

Confirmar gateways dos PC na respetiva interface .1 do router; não substituir
o OSPF por rotas estáticas entre routers. Esperar convergência e guardar a sessão.
Verificar nas consolas CORE (o caminho entre clientes e broker1 deve usar L1):

```sh
# pub1 e sub1
ip -br address
ip route
ping -c 3 10.0.3.20
traceroute 10.0.3.20
# broker1
ping -c 3 10.0.1.10
ping -c 3 10.0.2.31
```

## 2. Preparar execução e captura

Confirmar `python3 --version` (3.10+) e acesso à mesma cópia do projeto nos nós.
Definir em cada terminal `PROJECT` com o caminho absoluto real do projeto.
Não executar todos os papéis no terminal da VM: cada um corre no seu nó CORE.
Os exemplos usam logs em /tmp, diretoria existente; preservar os logs entre ensaios.

Iniciar a captura **antes** das aplicações, numa consola adicional do broker1:

```sh
tcpdump -i any -nn -s 0 -U -w /tmp/tp2-baseline.pcap 'tcp port 7310 or udp port 7311'
```

Em três consolas, broker1, sub1, pub1, por esta ordem e sem demoras longas:

```sh
# broker1
"$PROJECT/run.sh" broker --id broker1 --config "$PROJECT/configs/core/broker1.conf"
# sub1
"$PROJECT/run.sh" subscriber --id sub1 --config "$PROJECT/configs/core/subscriber1.conf"
# pub1
"$PROJECT/run.sh" publisher --id pub1 --config "$PROJECT/configs/core/publisher1.conf"
```

O publisher espera 5 s depois da sessão, publica 800 mensagens a 20/s, e dispõe de
10 s de drain. Subscriber e broker terminam aos 65 e 70 s. Não há stdout normal.
Para mostrar só uma publicação, copiar publisher1.conf para um caminho absoluto
em /tmp e mudar `count=800` para `count=1`; não alterar as chaves obrigatórias.
Para testar outras portas, mudar as duas no broker e só `broker_control` nos clientes.

Durante a execução, `tail -n 3 /tmp/tp2-sub1.log` demonstra flush imediato. Comparar
as duas cópias dos logs depois de todos terminarem; se /tmp for privado por nó,
copiar para uma diretoria comum da VM antes de comparar:

```sh
python3 "$PROJECT/tools/check_logs.py" /tmp/tp2-pub1.log /tmp/tp2-sub1.log
```

Este comparador pressupõe que sub1 subscreve todos os tópicos do publisher, como
neste cenário. Resultado: 800 publicações únicas, nenhuma em falta/inesperada.
Em G1, mais de 800 entregas é permitido; não deduplicar o log original.

## 3. Perdas e retransmissões

Reiniciar as aplicações para cada ensaio. Na configuração de L1 do CORE aplicar
10% de perda nas duas direções; verificar que a definição foi aplicada. Não
introduzir perdas em todas as interfaces nem alterar a topologia. Esperar novamente
pela conectividade. Repetir carga e captura, com nome `tp2-loss.pcap`.

Alternativa nas consolas r1 e r2, apenas se a VM permitir `tc`: identificar com
`ip -br address` a interface ligada a 10.0.11.0/24, guardar `tc qdisc show dev ...`
e aplicar em **ambas** as pontas da L1 (substituir o nome real):

```sh
L1_IF=nome_real_da_interface
tc qdisc show dev "$L1_IF"
tc qdisc replace dev "$L1_IF" root netem loss 10%
```

Não combinar esta alternativa com perdas configuradas na GUI. Depois do ensaio,
restaurar a configuração anterior da ligação; reiniciar a sessão CORE guardada é
uma forma de restaurar integralmente as definições. Voltar a testar conectividade.

Esperado: conjunto de publicações entregue por completo enquanto há tempo de
execução, retransmissões com mesmos IDs e possíveis duplicados. A perda aleatória
pode não perder especificamente um ACK numa execução curta. Para uma prova
determinística complementar **dentro do CORE**, usar o launcher exclusivo dos
testes, nas mesmas consolas, em vez de run.sh:

```sh
# broker1
TP2_TEST_LOSS=selected TP2_TEST_TRACE=/tmp/tp2-broker.trace python3 "$PROJECT/tests/loss_runner.py" broker --id broker1 --config "$PROJECT/configs/core/broker1.conf"
# sub1
TP2_TEST_LOSS=selected TP2_TEST_TRACE=/tmp/tp2-sub.trace python3 "$PROJECT/tests/loss_runner.py" subscriber --id sub1 --config "$PROJECT/configs/core/subscriber1.conf"
# pub1
TP2_TEST_LOSS=selected TP2_TEST_TRACE=/tmp/tp2-pub.trace python3 "$PROJECT/tests/loss_runner.py" publisher --id pub1 --config "$PROJECT/configs/core/publisher1.conf"
```

Este launcher suprime uma primeira tentativa de PUBLISH tx=1, DELIVER tx=1,
ACK do broker tx=2 e ACK do subscriber tx=3. Regista tentativas e descartes nos
traces; o restante tráfego usa sockets reais. Distinguir este ensaio de perdas
introduzidas pela rede CORE. A execução pelo contrato run.sh deve ser demonstrada
separadamente. Os traces são acrescentados: usar nomes novos em cada ensaio.

## 4. SIGSTOP/SIGCONT e evict_after=5

Fazer ensaios separados, inicialmente sem perda aleatória. Em bash interativo no
sub1, ativar job control para isolar o grupo que o avaliador suspenderá:

```sh
set -m
"$PROJECT/run.sh" subscriber --id sub1 --config "$PROJECT/configs/core/subscriber1.conf" &
sub_pid=$!
ps -o pid,pgid,stat,args -p "$sub_pid"
```

Confirmar PGID igual a `sub_pid`. O run.sh usa exec e não cria sessões/grupos novos.
Arrancar o publisher noutra consola e esperar que apareçam entregas. Na consola sub1:

```sh
# Ensaio curto, durante as publicações
kill -STOP -- "-$sub_pid"
sleep 2
kill -CONT -- "-$sub_pid"
wait "$sub_pid"
```

Esperado: mantém TCP, recupera todos os IDs em falta, pode registar duplicados e
continua a receber. Comparar logs no fim. Noutro ensaio, substituir `sleep 2` por
`sleep 8`. Havendo pendentes, o broker deve fechar TCP cerca de 5 s após deixar de
receber ACKs, parar os DELIVER e descartar a fila. Ao retomar, o subscriber termina.
Confirmar na captura que nenhuma publicação gerada após o fecho foi entregue;
não exigir igualdade com as 800 publicações nesse ensaio de expulsão.

Testar também subscriber sem publicações durante mais de 5 s: não deve ser expulso.
Depois arrancar o publisher e verificar que começa a receber. Enviar SIGTERM numa
execução separada confirma fecho rápido; as execuções normais devem terminar por si.

## 5. O que mostrar na captura e guardar

Parar tcpdump com Ctrl+C para fechar o ficheiro e abrir no Wireshark. Filtros de
visualização: `tcp.port == 7310`, `udp.port == 7311` ou `tcp.flags.fin == 1`.
Não há dissector personalizado: usar Follow TCP Stream e os bytes descritos em
`docs/protocol.md`; uma mensagem TCP pode atravessar vários segmentos.

- HELLO (tipo 1), WELCOME (2) completo antes do primeiro UDP do cliente;
  porta 7311 nos dois bytes após os oito bytes de sessão do WELCOME.
- SUBSCRIBE (3) e OK (5); PUBLISH (16), DELIVER (17), ACK (18).
- Cabeçalho comum: `43 43 01`, tipo no offset 3, tamanho no 4, txid no 8.
- Session ID muda no salto do broker; tópico/message_id permanecem iguais.
- Repetições a aproximadamente 250 ms, ACK associado ao txid correto,
  várias publicações antes de terminar a confirmação de todas.
- FIN do broker no ensaio de expulsão; nenhum DELIVER posterior para a sessão.

Guardar sessão CORE, versão da VM/Python/CORE, configurações exatas, condições de
perda, comandos, horários de STOP/CONT, ambos os logs e capturas baseline/perda/
suspensão curta/longa. Capturar também L1 permite ver perdas por comparação com
a captura no broker. A captura no broker, sozinha, não localiza todos os descartes.
