# Formato binário atual

Todos os inteiros são unsigned e big endian. Uma string usa comprimento de 2 bytes
seguido desse número de bytes UTF-8. Todos os campos abaixo são construídos e lidos
explicitamente em `protocol.py`; não se usa uma biblioteca de serialização.

## Cabeçalho comum

| Offset | Tamanho | Campo |
| --- | --- | --- |
| 0 | 2 | magic: bytes `CC` |
| 2 | 1 | versão: 1 |
| 3 | 1 | tipo de mensagem |
| 4 | 4 | comprimento do corpo em bytes |
| 8 | 8 | transmission_id |

O TCP pode dividir ou juntar mensagens: `pop_frame` retém dados incompletos e
consome uma mensagem de cada vez. No UDP, um datagrama contém exatamente uma
mensagem. O máximo total é 65507 bytes; valores maiores, versões/tipos inválidos,
truncamentos e bytes excedentes são rejeitados. Na prática, usar conteúdos pequenos
para não depender de fragmentação IP.

## Corpos

Os corpos descritos abaixo contêm os restantes campos de controlo binários
(cabeçalho variável da mensagem). Só `payload` é conteúdo opaco. Comprimentos de
strings contam bytes UTF-8, não caracteres. Não existem terminadores NUL.

| Tipo | Valor | Campos, pela ordem de transmissão |
| --- | --- | --- |
| HELLO | 1 | role (1), porta UDP do cliente (2), client_id (string) |
| WELCOME | 2 | session_id (8), porta UDP do broker (2), broker_id (string) |
| SUBSCRIBE | 3 | qos (1), filtro (string) |
| UNSUBSCRIBE | 4 | filtro (string) |
| OK | 5 | vazio |
| ERROR | 6 | motivo (string) |
| BYE | 7 | vazio |
| PUBLISH | 16 | session_id (8), qos (1), message_id (8), tópico (string), tamanho do payload (4), payload |
| DELIVER | 17 | mesmos campos de PUBLISH |
| ACK | 18 | session_id (8) |

HELLO a BYE usam TCP. PUBLISH, DELIVER e ACK usam UDP. Roles: publisher=1,
subscriber=2, broker=3 (este último ainda não suportado pelo broker).
Respostas de controlo e ACKs repetem o transmission_id do pedido/envio confirmado.
O session_id é o da sessão do cliente em cada salto; o broker substitui-o ao
encaminhar uma publicação. O message_id e o tópico mantêm-se de ponta a ponta.

Exemplo de sequência, com valores ilustrativos:

```text
subscriber -- HELLO tx=1, role=2, udp=40001, id=sub1 --> broker
subscriber <- WELCOME tx=1, session=22, udp=7311 ----- broker
subscriber -- SUBSCRIBE tx=2, qos=2, north/* --------> broker
subscriber <- OK tx=2 ------------------------------ broker
publisher --- HELLO tx=1, role=1, udp=40002, id=pub1 -> broker
publisher <-- WELCOME tx=1, session=11, udp=7311 ----- broker
publisher --- PUBLISH tx=1, session=11, msg=1, qos=1 -> broker
publisher <-- ACK tx=1, session=11 ------------------ broker
subscriber <- DELIVER tx=1, session=22, msg=1, qos=1 - broker
subscriber -- ACK tx=1, session=22 -----------------> broker
```

O ACK e o DELIVER podem ser observados em qualquer ordem nos destinatários. Se o
último ACK se perder, o broker reenvia DELIVER com os mesmos IDs; em G2 o subscriber
confirma de novo sem escrever outra linha. Em G1 escreve novamente.

## Sessões, validação e limites

O cliente abre TCP e um socket UDP local na interface escolhida pela ligação TCP.
HELLO anuncia a porta UDP efémera. O broker atribui uma sessão aleatória não nula
de 64 bits, sem colisões nas sessões ativas. O broker só aceita UDP depois de
escrever integralmente WELCOME no TCP; o cliente só inicia EventCast depois de
receber e validar WELCOME completo. Não é uma autenticação criptográfica.

Um datagrama só é aceite se a sessão, o papel e o par IPv4/porta de origem
corresponderem ao estabelecido. O cliente valida igualmente o endpoint UDP do
broker. ACK confirma o `transmission_id` no âmbito dessa sessão e salto; um ACK
desconhecido não confirma outra entrega nem adia uma expulsão.

`transmission_id` e `message_id` usam 64 bits. Os contadores locais começam em 1
e nunca dão a volta: esgotar IDs termina o publisher com erro, ou fecha apenas a
sessão do subscriber cujo contador de entregas esgotou. Ao esgotar IDs de controlo,
fecha-se TCP sem reutilizar o último ID para BYE. IDs de publicação reiniciam no
arranque; não há recuperação persistente. O par tópico/message_id identifica a
publicação, de acordo com a condição do enunciado de tópicos distintos por publisher.

WELCOME exige sessão e porta não nulas e um ID válido. IDs não contêm espaços nem
caracteres de controlo C0/C1/DEL e têm no máximo 65479 bytes. Tópicos/filtros têm
no máximo 65440 bytes, deixando espaço para os 23 bytes de campos da publicação
e para `reading=` seguido de um inteiro de 64 bits. Estas fronteiras são verificadas
antes de publicar. São limites de representação; para o CORE usar tópicos pequenos.

UDP truncado, com comprimentos incorretos, tipo/versão inválidos ou UTF-8 inválido
é descartado sem terminar o processo. Cabeçalho TCP inválido fecha essa ligação;
corpo inválido num pedido completo recebe ERROR quando possível. Um EOF com uma
mensagem parcial descarta-a e limpa a sessão. Frames TCP incompletos esperam mais
bytes, porque uma leitura parcial não significa uma mensagem malformada.

## Fiabilidade e temporizadores

| Mecanismo | Valor / origem | Efeito |
| --- | --- | --- |
| Retransmissão UDP | 250 ms | Reenvia apenas transmissões pendentes, com os mesmos bytes/IDs |
| Ciclo de eventos do cliente | Espera máxima 20 ms | Processa controlo, dados e geração concorrente |
| Ciclo do broker | Espera máxima 50 ms | Processa sessões, retransmissões e expulsão |
| Tentativas de ligação TCP | 100 ms | Permite arranque simultâneo dos papéis |
| Limite para estabelecer sessão | 10 s | Termina com erro se o broker não ficar disponível |
| Limite de resposta SubControl | 10 s | Termina com erro perante pedido sem resposta |
| `start_after` | Configuração | Conta desde WELCOME validado |
| `duration` | Configuração | Limita broker/subscriber; subscriber inclui espera por sessão |
| `drain` | Configuração | Máximo após última publicação; termina antes se tudo confirmado |
| `evict_after` | Configuração | Tempo sem progresso de ACK quando existem pendentes |

Relógio monótono para prazos; Unix em ms para logs. Não se espera por ACK para
gerar a próxima publicação: há pipelining e retransmissão seletiva. Não há janela
máxima nem controlo de congestionamento. Leituras de UDP e geração são limitadas
a 64 mensagens por passagem para permitir tratar outros eventos.

O primeiro pendente de um subscriber inicia `waiting_since`. Um ACK de um pendente
remove-o e reinicia esse prazo se restarem outros. Retransmissões, novas publicações
e ACKs desconhecidos não o reiniciam. Sem pendentes não há expulsão por inatividade.
Ao atingir `evict_after`, o broker fecha TCP e elimina sessão, filtros e pendentes.
O prazo tem a granularidade do ciclo de eventos e do escalonamento do SO.

Em G1, cada PUBLISH recebido pelo broker pode originar novas entregas; cada DELIVER
recebido e correspondido no subscriber escreve uma linha, mesmo repetido. Perder
ACK do publisher ou do subscriber pode assim produzir duplicados. Uma só chegada
correspondida por vários filtros origina uma entrega, com `min(qos_fluxo, max(qos_filtros))`.
Em G2 conservam-se as tabelas de duplicados já existentes. O subscriber faz flush
antes de confirmar; o publisher regista apenas a geração inicial.

G1 pressupõe que a comunicação recupera a tempo: perdas permanentes, fim de
`drain`/`duration` ou expulsão podem deixar publicações sem entrega. O ACK do broker
aceita a responsabilidade por encaminhar; não indica que o subscriber já confirmou.
