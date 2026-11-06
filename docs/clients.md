# Publisher e subscriber

Os dois clientes usam diretamente sockets TCP e UDP e apenas a biblioteca padrão
de Python (3.10 ou superior, tal como os tipos usados no broker existente).
`main.py` escolhe agora qualquer um dos três papéis.

## Experimentar

Na raiz `CC-26-27-BBB`, executar em três terminais, pela ordem indicada:

```sh
./run.sh broker --id broker1 --config "$PWD/configs/broker.conf"
./run.sh subscriber --id sub1 --config "$PWD/configs/subscriber.conf"
./run.sh publisher --id pub1 --config "$PWD/configs/publisher.conf"
```

Cada comando é para um terminal diferente. Os processos terminam automaticamente.
Os exemplos escrevem `/tmp/tp2-pub1.log` e `/tmp/tp2-sub1.log`; uma nova execução
substitui o ficheiro anterior. Sem perdas, esperar 40 publicações e 40 entregas,
apesar dos filtros sobrepostos. G1 pode registar duplicados com perdas de ACK.
O broker não cria ficheiro de registos.

Também é possível invocar `/caminho/CC-26-27-BBB/run.sh` de qualquer diretoria.
Os caminhos de configuração e de log devem ser absolutos. O diretório dos logs
deve existir antes do arranque.

Para o CORE, mudar `bind` do broker para `10.0.3.20` e `broker_control` dos clientes
para `10.0.3.20:7310`, e executar cada papel no respetivo nó da topologia.
As portas usadas são sempre as da configuração; a porta UDP do broker é descoberta
pela resposta WELCOME. Cada cliente anuncia no HELLO a sua porta UDP efémera.

## Organização

- `src/config.py`: configurações tipadas dos três papéis; valida chaves, valores,
  tópicos e opções obrigatórias, sem inventar valores de configuração.
- `src/client.py`: ligação TCP, delimitação de mensagens, descoberta da porta UDP,
  validação da origem dos datagramas e ciclo de eventos comum.
- `src/publisher/publisher.py`: geração dos fluxos e retransmissão de publicações.
- `src/subscriber/subscriber.py`: filtros, ações temporizadas, entrega e ACK.
- `src/protocol.py`: formato binário partilhado com o broker existente.

## Publisher

Depois do WELCOME, espera `start_after` segundos. Uma fila ordenada pelos próximos
instantes de envio permite gerar vários fluxos ao mesmo tempo, cada um com o seu
`count`, `rate`, tópico e qos. O conteúdo sintético é `reading=1`, `reading=2`, etc.

Cada publicação recebe um `message_id` de 64 bits. Cada envio inicial recebe também
um identificador de transmissão, de âmbito local à sessão. São contadores separados;
uma retransmissão conserva ambos. Não se reutilizam IDs quando esgotam os 64 bits:
o processo termina com erro. Os IDs de publicação reiniciam numa nova execução;
não há recuperação persistente de sessões neste trabalho.

O publisher guarda os datagramas ainda sem ACK e reenvia só esses a cada 250 ms.
Várias publicações podem estar em trânsito; não se espera pelo ACK de uma para
gerar a seguinte. Esta implementação não impõe uma janela máxima de pendentes,
pelo que o consumo de memória cresce com a carga e a duração de uma interrupção.

Cada mensagem é registada apenas no envio inicial. O ACK deve vir do endereço e
porta UDP do broker, conter a sessão correta e corresponder a um envio pendente.
Depois da última publicação, o processo termina assim que todos os ACKs chegam,
ou ao esgotar `drain`, mesmo que sobrem pendentes. Esse limite pode, por definição,
deixar mensagens por confirmar numa rede que perde todos os envios.
O ACK do broker significa que aceitou a publicação; as entregas aos subscribers
continuam a cargo do broker, independentemente da saída do publisher.

## Subscriber

Envia as subscrições iniciais depois do WELCOME. Envia também as ações `at` na hora
pedida, contada desde o arranque do processo, não desde a sessão. Ações cuja hora
já passou durante a ligação são enviadas assim que a sessão fica pronta. As ações
são ordenadas por tempo; empates preservam a ordem do ficheiro.

As mudanças locais de filtros acompanham o envio do pedido TCP. Uma entrega ainda
em trânsito depois de cancelar o último filtro correspondente é confirmada mas
não registada. O broker confirma os pedidos por OK e também remove os seus pendentes
sem interesse. Erros de controlo são apresentados em stderr e terminam o cliente.

Cada DELIVER contém o qos efetivo calculado pelo broker:

- G1: escreve uma linha por chegada, incluindo repetições; não consulta nem mantém
  estado de duplicados para essas entregas.
- G2: guarda `(topic, message_id)` em memória e escreve só na primeira entrega.
  Confirma também as repetições, para fazer parar as retransmissões do broker.

O `flush` acontece antes do ACK. Os registos usam Unix em milissegundos, o ID do
processo, o tópico e o message_id em decimal, separados por espaços. O relógio
monótono é usado apenas para temporizadores. O estado de duplicados dura a sessão;
não existe persistência nem reconexão automática após expulsão.

`duration` limita toda a vida do subscriber, incluindo a ligação inicial. Ao receber
o fecho da sessão TCP, deixa de consumir UDP e termina. SIGSTOP/SIGCONT funcionam
no próprio processo, sem threads nem processos destacados; SIGTERM/SIGINT pedem saída.

## Temporizadores e fecho

Além dos tempos configurados, o desenho usa repetição de ligação a cada 100 ms,
10 s para estabelecer a sessão, 10 s para respostas de controlo e 250 ms entre
retransmissões UDP. O ciclo consulta os eventos pelo menos a cada 20 ms quando
não há trabalho pesado. Os tempos internos são decisões de protocolo, não valores
por omissão para as chaves obrigatórias do ficheiro.

Ao terminar normalmente, tenta enviar BYE e fecha os sockets sem prolongar o prazo
de `duration`/`drain` à espera de OK. Se o TCP não aceitar todos os bytes naquele
instante, o fecho TCP permite ao broker limpar a sessão. Se a sessão do publisher
for fechada antes de terminar os envios/ACKs, o publisher termina com erro.

## Validação

```sh
python3 -m unittest discover -s tests -v
```

Os testes determinísticos incluem configurações, temporizadores, retransmissão
seletiva, G1/G2, ações `at` e trocas binárias com os handlers reais do broker,
introduzindo perdas de publicações, entregas e ACKs em memória.

`test_network.py` inicia os processos reais a partir de outra diretoria e verifica
fan-out, seletividade, filtros sobrepostos e logs. Se o ambiente proibir sockets,
esse teste é marcado como ignorado com a razão explícita. Isso não substitui a
execução no CORE, as capturas de tráfego nem os testes SIGSTOP/SIGCONT no ambiente
de demonstração. As ligações entre vários brokers continuam por implementar.
