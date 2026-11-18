# Percurso do código e processamento do protocolo

Este documento explica **como a implementação executa o protocolo**, seguindo o
caminho do código. Complementa [protocol.md](protocol.md), que é a especificação
do formato binário, e [clients.md](clients.md), que descreve a configuração e os
clientes. Os excertos foram retirados diretamente da implementação; omitem apenas
linhas sem impacto no fluxo explicado.

## 1. Arquitetura

O sistema é um *publish/subscribe* de broker único. Cada papel é um processo
Python independente e usa sockets não bloqueantes com `selectors`; não há threads.

```text
                 SubControl (TCP)
publisher  -------------------------->  broker
subscriber -------------------------->  broker

                 EventCast (UDP)
publisher  ---- PUBLISH / ACK --------> broker
subscriber <--- DELIVER / ACK --------> broker
```

O TCP existe para estabelecer e controlar a sessão: `HELLO`, `WELCOME`,
`SUBSCRIBE`, `UNSUBSCRIBE`, `OK`, `ERROR` e `BYE`. O UDP transporta os eventos:
`PUBLISH`, `DELIVER` e `ACK`.

O ponto de entrada escolhe a classe e o leitor de configuração corretos:

```python
implementations = {
    "broker": (Broker, load_broker_config),
    "publisher": (Publisher, load_publisher_config),
    "subscriber": (Subscriber, load_subscriber_config),
}
implementation, load_config = implementations[args.role]
process = implementation(args.id, load_config(args.config))
process.run()
```

O broker é o único ponto de encaminhamento. Não há comunicação direta entre
publisher e subscriber, nem entre brokers: uma configuração com `peer` é
explicitamente recusada.

```python
if config.peers:
    raise ValueError("peer connections belong to phase 4 and are not implemented")
```

## 2. Representação binária comum

Todas as mensagens começam com um cabeçalho de 16 bytes. Os campos numéricos são
*unsigned* e big-endian (ordem de rede):

```python
MAGIC = b"CC"
VERSION = 1
HEADER_SIZE = 16

return (MAGIC + uint(VERSION, 1) + uint(kind, 1)
        + uint(len(body), 4) + uint(transmission_id, 8) + body)
```

| Offset | Tamanho | Campo |
| ---: | ---: | --- |
| 0 | 2 | `CC`, identificador do protocolo |
| 2 | 1 | versão, atualmente `1` |
| 3 | 1 | tipo (`Kind`) |
| 4 | 4 | comprimento do corpo em bytes |
| 8 | 8 | `transmission_id` |

O `transmission_id` identifica uma transmissão **num salto**. Não é o mesmo que
o `message_id`: o primeiro associa ACKs a um datagrama; o segundo identifica a
publicação lógica e mantém-se desde o publisher até ao subscriber.

### TCP não preserva limites de mensagens

Uma chamada `recv()` pode conter parte de uma mensagem ou várias. `pop_frame`
retira somente uma frame completa e mantém os bytes restantes no `bytearray`:

```python
def pop_frame(buffer):
    if len(buffer) < HEADER_SIZE:
        return None
    kind, size, transmission_id = read_header(buffer)
    end = HEADER_SIZE + size
    if len(buffer) < end:
        return None
    frame = Frame(kind, transmission_id, bytes(buffer[HEADER_SIZE:end]))
    del buffer[:end]
    return frame
```

Logo, fragmentação TCP não é tratada como erro: o processo espera pelos bytes em
falta. Depois de uma frame completa, o ciclo chama `pop_frame` novamente, tratando
coalescência de várias mensagens na mesma leitura.

### UDP preserva o datagrama

Em UDP cada datagrama deve ser exatamente uma mensagem completa. Comprimento a
menos ou bytes extra invalidam-no:

```python
def decode_datagram(data):
    kind, size, transmission_id = read_header(data)
    if len(data) != HEADER_SIZE + size:
        raise ProtocolError("datagram length mismatch")
    return Frame(kind, transmission_id, data[HEADER_SIZE:])
```

O limite é `65507` bytes, o máximo de payload UDP IPv4. Na prática os payloads
devem ser pequenos para evitar fragmentação IP.

## 3. Estabelecimento da sessão

Antes de qualquer evento UDP, o cliente abre TCP para `broker_control`, cria um
socket UDP local e anuncia a sua porta UDP no `HELLO`:

```python
self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
self.udp.bind((self.tcp.getsockname()[0], 0))
self.udp.setblocking(False)
self.selector.register(self.udp, selectors.EVENT_READ, "udp")
self._request(Kind.HELLO, hello_body(
    self.role, self.udp.getsockname()[1], self.client_id))
```

O porto `0` pede ao sistema operativo uma porta efémera. O corpo `HELLO` contém
`role:u8`, `data_port:u16` e `client_id:text`.

No broker, `HELLO` tem obrigatoriamente de ser a primeira mensagem TCP. O broker
associa a porta anunciada ao mesmo endereço IPv4 que abriu a ligação TCP, evitando
que um cliente registe arbitrariamente um endpoint de outra máquina:

```python
if client.role is None:
    if frame.kind != Kind.HELLO:
        raise ProtocolError("HELLO must be the first message")
    role, port, client_id = parse_hello(frame.body)
    if role == Role.BROKER:
        raise ProtocolError("peer sessions are not implemented")
    endpoint = (client.address[0], port)
```

Também proíbe IDs de cliente e endpoints UDP duplicados e cria um token aleatório
de 64 bits, não nulo e sem colisão entre sessões ativas:

```python
if any(c.client_id == client_id or c.endpoint == endpoint
       for c in self.sessions.values()):
    raise ProtocolError("client ID or UDP endpoint already in use")
session_id = secrets.randbits(64)
while not session_id or session_id in self.sessions:
    session_id = secrets.randbits(64)
```

Depois guarda a sessão e responde `WELCOME`. A resposta mantém o
`transmission_id` do `HELLO` para o cliente a poder correlacionar:

```python
client.session_id = session_id
self.sessions[session_id] = client
self._reply(client, Kind.WELCOME, frame.transmission_id,
            welcome_body(session_id, self.config.data_port, self.broker_id))
```

O cliente só fica `ready` após receber e validar o `WELCOME`; assim aprende o
porto UDP do broker:

```python
session_id, data_port, broker_id = parse_welcome(frame.body)
self.session_id = session_id
self.data_address = (self.config.broker_control[0], data_port)
self.ready = True
self._session_ready(time.monotonic())
```

O broker só aceita UDP do cliente depois de esvaziar a saída TCP, o que garante
que o `WELCOME` foi escrito:

```python
if not client.outgoing:
    # WELCOME has actually been written before UDP is accepted.
    client.ready = client.role is not None
```

Sequência mínima:

```text
Cliente                                Broker
  | -- TCP HELLO (tx=1, udp=40001) --> |
  | <-- TCP WELCOME (tx=1, session=S) -|
  | -- UDP PUBLISH / ACK ------------> |
  | <-- UDP DELIVER / ACK ------------ |
```

## 4. Sessão, origem e validação de UDP

Receber o `session_id` correto não é suficiente. Para cada datagrama o broker
confere simultaneamente a sessão, o papel, o estado e a origem IP/porto:

```python
@staticmethod
def _valid_sender(client, address, role):
    return (client is not None and client.ready and not client.closing
            and client.role == role and client.endpoint == address)
```

No caminho de `PUBLISH`, a sessão indicada no corpo escolhe o candidato; só depois
esta verificação permite encaminhar:

```python
publication = parse_publication(frame.body)
client = self.sessions.get(publication.session_id)
if not self._valid_sender(client, address, Role.PUBLISHER):
    continue
self._publish(client, frame.transmission_id, publication)
```

Para `ACK`, o `session_id` aponta para um subscriber e o ACK só remove a entrega
cujo `transmission_id` está efetivamente pendente:

```python
client = self.sessions.get(parse_ack(frame.body))
if not self._valid_sender(client, address, Role.SUBSCRIBER):
    continue
if frame.transmission_id in client.pending:
    del client.pending[frame.transmission_id]
```

O cliente faz a verificação simétrica: descarta qualquer datagrama UDP que não
venha do endereço e porto aprendidos no `WELCOME`.

```python
raw, source = self.udp.recvfrom(MAX_DATAGRAM + 1)
if source != self.data_address:
    continue
self._datagram(decode_datagram(raw))
```

Isto evita aceitar tráfego casual ou ACKs de outra sessão, mas não é autenticação
criptográfica e não resolve cenários NAT.

## 5. Subscrições e seleção por tópico

Depois da sessão, o subscriber envia as subscrições configuradas. Cada pedido
TCP recebe um `transmission_id` de controlo e fica na tabela `requests` até obter
resposta ou exceder 10 segundos:

```python
txid = self.next_control_id
self.next_control_id += 1
self.outgoing.extend(encode_frame(kind, txid, body))
self.requests[txid] = (kind, time.monotonic() + CONTROL_TIMEOUT)
```

O broker guarda, por subscriber, um dicionário `filtro -> qos`:

```python
topic_filter, qos = parse_subscription(frame.body)
client.subscriptions[topic_filter] = qos
self._reply(client, Kind.OK, frame.transmission_id)
```

Os tópicos de publicação têm três níveis, por exemplo
`north/station7/temperature`. Um filtro pode ser exato ou acabar em `/*`, com um
ou dois níveis antes do asterisco. Como os filtros são validados, a correspondência
é simples:

```python
def matches(topic, topic_filter):
    return topic.startswith(topic_filter[:-1]) if topic_filter.endswith("/*") else topic == topic_filter
```

`UNSUBSCRIBE` remove o filtro e também cancela retransmissões de mensagens que já
não interessam ao subscriber:

```python
client.subscriptions.pop(topic_filter, None)
for txid, pending in list(client.pending.items()):
    if not any(matches(pending.topic, f) for f in client.subscriptions):
        del client.pending[txid]
```

Se um `DELIVER` já estiver no socket quando o cancelamento acontece, o subscriber
não o escreve no log, mas confirma-o para impedir retransmissões desnecessárias.

## 6. Publicação e fan-out

O publisher usa uma heap para agendar fluxos independentes por `count` e `rate`.
Quando chega o instante de um evento, atribui **dois** identificadores:

```python
message_id, txid = self.next_message_id, self.next_transmission_id
self.next_message_id += 1
self.next_transmission_id += 1
payload = f"reading={generated + 1}".encode("utf-8")
packet = encode_frame(Kind.PUBLISH, txid, publication_body(
    self.session_id, flow.qos, message_id, flow.topic, payload))
```

- `message_id`: identidade lógica do evento, preservada através do broker;
- `txid`: identidade desta transmissão publisher → broker, usada pelo ACK;
- `session_id`: token da sessão do publisher neste salto.

O publisher guarda o pacote antes de o enviar, para ser capaz de o retransmitir,
e só regista a publicação na primeira geração:

```python
self.pending[txid] = Pending(packet, now)
self._send_udp(packet)
self._write_log(flow.topic, message_id)
```

O broker percorre os subscribers prontos e recolhe todos os QoS dos filtros que
correspondem. Um subscriber recebe **uma única entrega**, mesmo que vários filtros
correspondam ao tópico:

```python
guarantees = [qos for topic_filter, qos in subscriber.subscriptions.items()
              if matches(publication.topic, topic_filter)]
if not guarantees:
    continue
qos = min(publication.qos, max(guarantees))
```

O `max` escolhe a melhor subscrição aplicável e o `min` impede que o broker ofereça
uma garantia superior ao QoS da publicação. Por exemplo, publicação G2 e filtros
G1/G2 resulta em G2; publicação G1 e filtro G2 resulta em G1.

Para cada subscriber, o broker cria um `txid` de entrega independente e troca o
`session_id` pelo da sessão daquele subscriber:

```python
txid = subscriber.next_transmission_id
subscriber.next_transmission_id += 1
packet = encode_frame(Kind.DELIVER, txid, publication_body(
    subscriber.session_id, qos, publication.message_id,
    publication.topic, publication.payload))
subscriber.pending[txid] = Pending(packet, publication.topic, now)
self._send_udp(packet, subscriber.endpoint)
```

Assim, numa mesma publicação para dois subscribers, tópico, payload e
`message_id` são iguais, mas `session_id` e `transmission_id` de cada `DELIVER`
são diferentes.

## 7. ACK, retransmissão e QoS

Há dois saltos fiáveis independentes:

```text
publisher -- PUBLISH(tx=P) --> broker -- ACK(tx=P) --> publisher
broker    -- DELIVER(tx=S) --> subscriber -- ACK(tx=S) --> broker
```

O ACK ao publisher é enviado depois de o broker aceitar e processar o `PUBLISH`.
Não espera que os subscribers confirmem:

```python
# A publisher ACK means accepted by the broker, not all recipients finished.
self._send_udp(encode_frame(Kind.ACK, transmission_id,
                             uint(publisher.session_id, 8)), publisher.endpoint)
```

### G1: pelo menos uma vez

Para QoS 1, cada `PUBLISH` recebido é encaminhado, mesmo que seja a repetição do
mesmo datagrama. Consequentemente, perder o ACK do broker pode provocar uma nova
entrega lógica; perder o ACK do subscriber pode provocar um `DELIVER` repetido.

```python
identity = (publication.topic, publication.message_id)
if publication.qos == 1 or identity not in publisher.seen:
    # cria os DELIVER para todos os subscribers interessados
    ...
```

No subscriber G1, cada chegada interessada é escrita, incluindo repetidas:

```python
if interested and (publication.qos == 1 or identity not in self.seen):
    self._write_log(publication.topic, publication.message_id)
```

### G2: deduplicação em memória

Para QoS 2, o broker memoriza `(topic, message_id)` na sessão do publisher. Uma
repetição recebe ACK, mas não cria uma segunda entrega lógica:

```python
if publication.qos == 2:
    publisher.seen.add(identity)
```

O subscriber também memoriza essa identidade e escreve somente a primeira chegada
G2. Porém, confirma todas as cópias, inclusive duplicadas ou já canceladas:

```python
if interested and (publication.qos == 1 or identity not in self.seen):
    self._write_log(publication.topic, publication.message_id)
    if publication.qos == 2:
        self.seen.add(identity)
self._send_udp(encode_frame(Kind.ACK, frame.transmission_id,
                             uint(self.session_id, 8)))
```

O `flush` no `_write_log` ocorre antes do ACK. Portanto, receber um ACK de
subscriber significa que a linha de log correspondente já foi aceite pelo processo.

### Retransmissão seletiva

O publisher reenvia cada `PUBLISH` ainda pendente a cada 250 ms, sem bloquear
novas publicações:

```python
for pending in self.pending.values():
    if now - pending.sent_at >= RETRY_INTERVAL:
        self._send_udp(pending.packet)
        pending.sent_at = now
```

O broker aplica a mesma regra, mas à tabela de pendentes de **cada subscriber**:

```python
for pending in client.pending.values():
    if now - pending.sent_at >= RETRY_INTERVAL:
        self._send_udp(pending.packet, client.endpoint)
        pending.sent_at = now
```

Como cada transmissão tem uma entrada própria, confirmar uma entrega não confirma
as outras. Várias publicações podem estar em trânsito ao mesmo tempo.

## 8. Evicção, fecho e temporizadores

Um subscriber só é expulso se tiver pendentes e não fizer progresso de ACK durante
`evict_after`. O instante começa no primeiro pendente e só é renovado quando um
ACK válido remove realmente uma entrega:

```python
if frame.transmission_id in client.pending:
    del client.pending[frame.transmission_id]
    client.waiting_since = time.monotonic() if client.pending else None
```

O temporizador desconecta esse cliente sem bloquear os restantes:

```python
if now - client.waiting_since >= self.config.evict_after:
    self._disconnect(client)
    continue
```

Ao desconectar, o broker remove o mapeamento de sessão e limpa as estruturas que
não podem sobreviver ao fecho:

```python
self.sessions.pop(client.session_id, None)
client.ready = False
client.closing = True
client.waiting_since = None
client.pending.clear()
client.seen.clear()
client.subscriptions.clear()
```

`BYE` marca o cliente como em fecho, deixa de aceitar UDP e responde `OK`; EOF TCP
tem o mesmo efeito prático de limpar a sessão. O subscriber não tenta reconectar
após expulsão, para não voltar a consumir datagramas de uma sessão inválida.

Os prazos usam `time.monotonic()`, que não é afetado por alterações do relógio do
sistema. Os logs usam Unix em milissegundos, pois representam o instante observável
da publicação ou entrega.

| Temporizador | Valor | Função |
| --- | --- | --- |
| retransmissão UDP | 250 ms | reenvia datagramas ainda pendentes |
| tentativa TCP | 100 ms | permite arranque próximo dos papéis |
| sessão inicial | 10 s | falha se não chegar `WELCOME` válido |
| resposta de controlo | 10 s | falha pedido TCP sem resposta |
| ciclo do cliente | até 20 ms | trata TCP, UDP, agenda e prazos |
| ciclo do broker | até 50 ms | trata sockets, retries e expulsão |

## 9. Máquina de estados resumida

```text
Cliente
  iniciar -> ligar TCP -> enviar HELLO -> receber WELCOME -> ready
  ready publisher  -> gerar PUBLISH -> aguardar ACK / retransmitir -> drain -> fechar
  ready subscriber -> SUBSCRIBE/at -> receber DELIVER -> log/ACK -> fechar

Broker, por cliente
  aceitar TCP -> aguardar HELLO -> escrever WELCOME -> ready
  publisher ready  -> aceitar PUBLISH -> fan-out -> ACK ao publisher
  subscriber ready -> aceitar SUBSCRIBE/UNSUBSCRIBE; retransmitir DELIVER até ACK
  BYE/EOF/evicção -> remover sessão e limpar estado
```

## 10. Limites importantes da implementação

- A fiabilidade depende de a rede recuperar antes de `drain`, `duration` ou
  `evict_after`; perdas permanentes podem deixar eventos não confirmados.
- Não há persistência. Reiniciar publisher, broker ou subscriber perde sessões,
  pendentes e tabelas G2; G2 só evita duplicados durante a sessão em memória.
- Não há janela máxima de pendentes nem controlo de congestionamento. Uma carga
  elevada ou um subscriber parado pode aumentar a memória e atrasar temporizadores.
- O RTO é fixo em 250 ms: é simples e previsível, mas não se adapta a RTT ou carga.
- A validação de origem IP/porto e `session_id` reduz tráfego inválido, mas não
  substitui autenticação, integridade criptográfica ou suporte NAT.
- Ligações `peer` e encaminhamento entre brokers não estão implementados.

Para verificar estes percursos, consultar os testes e executar:

```sh
python3 -m unittest discover -s tests -v
```

Os testes cobrem framing TCP, datagramas inválidos, filtros, fan-out, perdas de
ACK, retransmissão, G1/G2, cancelamento, evicção e encerramento. A validação com
perdas reais e capturas na topologia CORE permanece descrita em [core.md](core.md).
