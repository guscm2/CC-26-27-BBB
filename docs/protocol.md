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
publisher --- PUBLISH tx=1, session=11, msg=1, qos=2 -> broker
publisher <-- ACK tx=1, session=11 ------------------ broker
subscriber <- DELIVER tx=1, session=22, msg=1, qos=2 - broker
subscriber -- ACK tx=1, session=22 -----------------> broker
```

O ACK e o DELIVER podem ser observados em qualquer ordem nos destinatários. Se o
último ACK se perder, o broker reenvia DELIVER com os mesmos IDs; em G2 o subscriber
confirma de novo sem escrever outra linha. Em G1 escreve novamente.
