MC714 - Trabalho 2

Este projeto implementa algoritmos distribuídos usando comunicação via sockets TCP.

Cada processo representa um nó do sistema distribuído. Os nós trocam mensagens em formato JSON pela rede local.

Algoritmos implementados

1. Relógio lógico de Lamport
2. Exclusão mútua distribuída com Ricart-Agrawala
3. Eleição de líder com algoritmo Bully

Arquivos do projeto

* node.py: implementação principal dos nós, comunicação por sockets e algoritmos distribuídos.
* config.py: configuração dos nós e portas utilizadas.
* relatorio.md: relatório explicando a solução, os testes realizados e as fontes utilizadas.

Requisitos

Este projeto foi implementado em Python 3 e utiliza apenas bibliotecas padrão da linguagem:

* socket
* threading
* json
* sys

Portanto, não é necessário instalar bibliotecas externas.

Como executar

Abra três terminais na pasta do projeto.

No primeiro terminal, execute:

python node.py 1

No segundo terminal, execute:

python node.py 2

No terceiro terminal, execute:

python node.py 3

Cada terminal representa um nó independente do sistema distribuído.

Configuração dos nós

Os nós são definidos no arquivo config.py.

Exemplo de configuração:

NODES = {
1: (“localhost”, 5001),
2: (“localhost”, 5002),
3: (“localhost”, 5003),
}

Nesse caso:

* O Nó 1 escuta em localhost:5001
* O Nó 2 escuta em localhost:5002
* O Nó 3 escuta em localhost:5003

Comandos disponíveis

Durante a execução de cada nó, é possível utilizar os seguintes comandos:

send <id_do_destino> 

request_cs

release_cs

leader

election

exit

Comando send

Envia uma mensagem comum para outro nó.

Exemplo:

send 2 primeira mensagem

Esse comando envia uma mensagem do nó atual para o Nó 2.

Comando request_cs

Solicita entrada na região crítica.

Exemplo:

request_cs

O nó envia mensagens REQUEST para os outros nós e aguarda mensagens REPLY.

Comando release_cs

Libera a região crítica.

Exemplo:

release_cs

Ao sair da região crítica, o nó envia respostas pendentes, caso existam.

Comando leader

Mostra qual líder o nó conhece atualmente.

Exemplo:

leader

Comando election

Inicia uma eleição de líder usando o algoritmo Bully.

Exemplo:

election

Comando exit

Encerra o nó.

Exemplo:

exit

Formato das mensagens

As mensagens são enviadas em formato JSON.

Exemplo de mensagem:

{
“type”: “MESSAGE”,
“sender”: 1,
“clock”: 1,
“content”: “primeira mensagem”
}

Os principais campos são:

* type: tipo da mensagem
* sender: identificador do nó remetente
* clock: valor do relógio lógico de Lamport
* content: conteúdo da mensagem

Tipos de mensagem utilizados:

* MESSAGE: mensagem comum entre nós
* REQUEST: solicitação para entrar na região crítica
* REPLY: permissão para entrada na região crítica
* ELECTION: mensagem de início de eleição
* OK: resposta de um nó ativo durante a eleição
* COORDINATOR: mensagem informando o novo líder

Relógio lógico de Lamport

Cada nó possui um relógio lógico local.

Quando um nó envia uma mensagem, ele incrementa seu relógio antes do envio.

Quando um nó recebe uma mensagem, ele atualiza seu relógio usando a regra:

clock_local = max(clock_local, clock_recebido) + 1

Essa regra permite ordenar eventos distribuídos sem depender de relógios físicos sincronizados.

Teste do relógio de Lamport

Com os nós 1 e 2 em execução, no terminal do Nó 1 execute:

send 2 primeira mensagem

O Nó 1 incrementa seu relógio lógico e envia a mensagem para o Nó 2.

O Nó 2 recebe a mensagem, lê o relógio recebido e atualiza seu relógio local.

Exemplo esperado no Nó 1:

Nó 1 enviou mensagem para Nó 2 | clock=1

Exemplo esperado no Nó 2:

Nó 2 recebeu MESSAGE de Nó 1: primeira mensagem | clock recebido=1 | clock atual=2

Exclusão mútua distribuída

Foi implementado o algoritmo de Ricart-Agrawala.

A região crítica representa um trecho em que apenas um nó pode entrar por vez.

Quando um nó quer entrar na região crítica:

1. Incrementa seu relógio lógico.
2. Guarda o timestamp do pedido.
3. Envia REQUEST para todos os outros nós.
4. Aguarda receber REPLY de todos.
5. Entra na região crítica.

A prioridade é definida pelo par:

(timestamp, node_id)

O menor timestamp tem prioridade. Em caso de empate, vence o menor ID.

Teste da exclusão mútua

Com os três nós em execução, no terminal do Nó 1 execute:

request_cs

O Nó 1 enviará REQUEST para os nós 2 e 3.

Os nós 2 e 3 responderão com REPLY.

Quando o Nó 1 receber todas as permissões, ele entrará na região crítica.

Exemplo esperado:

*** Nó 1 ENTROU na região crítica ***

Para sair da região crítica, execute no Nó 1:

release_cs

Exemplo esperado:

*** Nó 1 SAIU da região crítica ***

Teste de disputa pela região crítica

Também é possível testar dois nós tentando entrar na região crítica.

Exemplo:

No Nó 1:

request_cs

Logo depois, no Nó 2:

request_cs

O algoritmo decide quem entra primeiro usando o timestamp de Lamport e o ID do nó.

Apenas um nó entra na região crítica por vez.

Quando o primeiro nó executar:

release_cs

o outro nó poderá entrar na região crítica.

Eleição de líder com Bully

Foi implementado o algoritmo Bully para eleição de líder.

Inicialmente, o líder é o nó com maior ID.

Com três nós:

Nó 1

Nó 2

Nó 3

o líder inicial é o Nó 3.

Verificar líder atual

Em qualquer nó, execute:

leader

Exemplo esperado:

Líder atual conhecido pelo Nó 1: Nó 3

Simular falha do líder

Para simular a falha do líder, encerre o processo do Nó 3 com:

CTRL + C

Depois, no Nó 2, execute:

election

O Nó 2 tentará se comunicar com nós de ID maior.

Como o Nó 3 foi encerrado, o Nó 2 não conseguirá contato com ele e se declarará novo líder.

Exemplo esperado no Nó 2:

Nó 2 iniciou eleição.
Nó 2 não conseguiu contato com Nó 3
*** Nó 2 virou o novo LÍDER ***
Nó 2 enviou COORDINATOR para Nó 1

Exemplo esperado no Nó 1:

*** Nó 1 reconhece Nó 2 como novo LÍDER ***

Demonstrações realizadas

Foram realizados os seguintes testes:

1. Comunicação entre nós usando sockets TCP.
2. Envio de mensagens JSON.
3. Atualização do relógio lógico de Lamport.
4. Entrada e saída da região crítica.
5. Disputa entre dois nós pela região crítica.
6. Eleição de novo líder após falha simulada do líder inicial.

Observações

A execução foi feita localmente, com múltiplos processos Python rodando em terminais diferentes.

Embora os nós estejam na mesma máquina, cada processo possui sua própria porta TCP e se comunica com os demais por troca de mensagens via socket.

A falha de um nó foi simulada encerrando seu processo com CTRL + C.