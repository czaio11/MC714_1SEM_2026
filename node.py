import json
import socket # serve para um processo se comunicar com outro
import sys
import threading # serve para rodar dois processos ao mesmo tempo
from config import NODES

lamport_clock = 0 # quando o nó inicia, o relogio comeca em 0

requesting_cs = False # verifica o nó está tentando entrar na região crítica.
inside_cs = False # verifica o nó já está dentro da região crítica.
request_timestamp = None # Guarda o clock de Lamport no momento em que o nó pediu a região crítica.
replies_received = set() # Guarda de quais nós ele já recebeu permissão.
deferred_replies = set() # Guarda para quais nós ele deve responder depois, porque no momento atual ele tem prioridade.

leader_id = max(NODES.keys())

def start_node(node_id):
    host, port = NODES[node_id] # pega a porta do nó no config.py.

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # cria um socket TCP.

    server_socket.bind((host, port)) # aloca o nó a uma porta

    server_socket.listen() # aguarda para receber conexoes

    print(f"Nó {node_id} escutando em {host}:{port}")

    while True: # fica esperando alguém conectar
        connection, address = server_socket.accept() # enquanto nao se concectar ao nó, o programa é bloqueado

        data = connection.recv(1024).decode() # recebe os dados de até 1024 bytes

        if data: # basicamente mostra a mensagem recebida, transformando o json em dicionario
            try:
                packet = json.loads(data)

                sender = packet["sender"]
                message_type = packet["type"]
                content = packet["content"]
                received_clock = packet["clock"]

                global lamport_clock
                global deferred_replies

                lamport_clock = max(lamport_clock, received_clock) + 1 # O relógio local deve avançar para depois do evento recebido, basicamente verifica se precisa ataulizar ou nao

                print(f"\nNó {node_id} recebeu {message_type} de Nó {sender}: {content} "f"| clock recebido={received_clock} | clock atual={lamport_clock}")

                if message_type == "MESSAGE":
                    pass

                elif message_type == "REQUEST":

                    my_priority = (request_timestamp, node_id)

                    other_priority = (received_clock, sender)

                    if inside_cs:

                        deferred_replies.add(sender)

                        print(f"Nó {node_id} adiou REPLY para Nó {sender}, pois está na região crítica.")

                    elif requesting_cs and my_priority < other_priority:

                        deferred_replies.add(sender)

                        print(f"Nó {node_id} adiou REPLY para Nó {sender}, pois tem prioridade.")

                    else:

                        send_reply(node_id, sender)

                elif message_type == "REPLY":

                    replies_received.add(sender)

                    print(f"Nó {node_id} recebeu permissão de Nó {sender}. Permissões: {replies_received}")

                    check_enter_critical_section(node_id)
                elif message_type == "ELECTION":
                    print(f"Nó {node_id} recebeu ELECTION de Nó {sender}")

                    send_ok(node_id, sender)

                    if node_id > sender:
                        start_election(node_id)

                elif message_type == "OK":
                    print(f"Nó {node_id} recebeu OK de Nó {sender}")

                elif message_type == "COORDINATOR":
                    global leader_id

                    leader_id = sender

                    print(f"*** Nó {node_id} reconhece Nó {leader_id} como novo LÍDER ***")
                
            except json.JSONDecodeError:
                print(f"\nNó {node_id} recebeu mensagem inválida, não era JSON: {data}")

            print("> ", end="", flush=True)

        connection.close()

def send_packet(sender_id, target_id, packet): # recebe um dicionário Python, transforma em JSON e envia.
    host, port = NODES[target_id] # pega a porta do nó defininido no target_id

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # cria um socket para enviar mensagem

    try:
        client_socket.connect((host, port))
        client_socket.send(json.dumps(packet).encode())
        return True

    except ConnectionRefusedError:
        return False

    finally:
        client_socket.close()
    

def send_message(sender_id, target_id, message): # envia mensagem de um nó para outro
    global lamport_clock # queremos modificar ela fora dessa funcao
    lamport_clock += 1 # Antes de enviar uma mensagem, o processo incrementa seu relógio.

    packet = { # Estrutura nossa mensagem
        "type" : "MESSAGE",
        "sender": sender_id,
        "clock": lamport_clock,
        "content": message
    }
    success = send_packet(sender_id, target_id, packet)

    if success:
        print(f"Nó {sender_id} enviou mensagem para Nó {target_id} | clock={lamport_clock}")
    else:
        print(f"Erro: Nó {target_id} não está rodando.")


def send_reply(sender_id, target_id):
    global lamport_clock

    lamport_clock += 1

    packet = {
        "type": "REPLY",
        "sender": sender_id,
        "clock": lamport_clock,
        "content": "Permissão concedida"
    }
    success = send_packet(sender_id, target_id, packet)

    if success:
        print(f"Nó {sender_id} enviou REPLY para Nó {target_id} | clock={lamport_clock}")
    else:
        print(f"Erro: Nó {target_id} não está rodando.")

def send_ok(sender_id, target_id):
    global lamport_clock

    lamport_clock += 1

    packet = {
        "type": "OK",
        "sender": sender_id,
        "clock": lamport_clock,
        "content": "Estou ativo"
    }

    success = send_packet(sender_id, target_id, packet)

    if success:
        print(f"Nó {sender_id} enviou OK para Nó {target_id} | clock={lamport_clock}")

def request_critical_section(node_id):
    global lamport_clock
    global requesting_cs
    global inside_cs
    global request_timestamp
    global replies_received

    if inside_cs:
        print(f"Nó {node_id} já está dentro da região crítica.")
        return

    if requesting_cs:
        print(f"Nó {node_id} já está tentando entrar na região crítica.")
        return

    requesting_cs = True
    replies_received = set()

    lamport_clock += 1
    request_timestamp = lamport_clock

    print(f"Nó {node_id} solicitou entrada na região crítica | timestamp={request_timestamp}")

    for target_id in NODES:
        if target_id == node_id:
            continue

        packet = {
            "type": "REQUEST",
            "sender": node_id,
            "clock": lamport_clock,
            "content": "Quero entrar na região crítica"
        }
        success = send_packet(node_id, target_id, packet)

        if success:
            print(f"Nó {node_id} enviou REQUEST para Nó {target_id} | clock={lamport_clock}")
        else:
            print(f"Erro: Nó {target_id} não está rodando.")

def check_enter_critical_section(node_id):
    global requesting_cs
    global inside_cs

    expected_replies = set(NODES.keys()) - {node_id}

    if requesting_cs and replies_received == expected_replies:
        requesting_cs = False
        inside_cs = True

        print(f"\n*** Nó {node_id} ENTROU na região crítica ***")
        print("> ", end="", flush=True)

def release_critical_section(node_id):
    global inside_cs
    global deferred_replies

    if not inside_cs:
        print(f"Nó {node_id} não está dentro da região crítica.")
        return

    inside_cs = False

    print(f"*** Nó {node_id} SAIU da região crítica ***")

    for target_id in deferred_replies:
        send_reply(node_id, target_id)

    deferred_replies = set()


# Procura nós com ID maior, Tenta mandar ELECTION para eles, Se ninguém maior responder/conectar, vira líder, Se algum maior estiver vivo, espera ele assumir.
def start_election(node_id):
    global leader_id
    global lamport_clock

    print(f"Nó {node_id} iniciou eleição.")

    higher_nodes = [other_id for other_id in NODES if other_id > node_id]

    alive_higher_nodes = []

    for target_id in higher_nodes:
        lamport_clock += 1

        packet = {
            "type": "ELECTION",
            "sender": node_id,
            "clock": lamport_clock,
            "content": "Eleição iniciada"
        }

        success = send_packet(node_id, target_id, packet)

        if success:
            alive_higher_nodes.append(target_id)
            print(f"Nó {node_id} enviou ELECTION para Nó {target_id} | clock={lamport_clock}")
        else:
            print(f"Nó {node_id} não conseguiu contato com Nó {target_id}")

    if len(alive_higher_nodes) == 0:
        leader_id = node_id
        print(f"*** Nó {node_id} virou o novo LÍDER ***")

        announce_coordinator(node_id)

    else:
        print(f"Nó {node_id} encontrou nós maiores ativos: {alive_higher_nodes}")
        print(f"Nó {node_id} aguarda um nó maior assumir a liderança.")

# avisa que é o novo lider
def announce_coordinator(node_id):
    global lamport_clock

    for target_id in NODES:
        if target_id == node_id:
            continue

        lamport_clock += 1

        packet = {
            "type": "COORDINATOR",
            "sender": node_id,
            "clock": lamport_clock,
            "content": "Sou o novo líder"
        }

        success = send_packet(node_id, target_id, packet)

        if success:
            print(f"Nó {node_id} enviou COORDINATOR para Nó {target_id} | clock={lamport_clock}")

def command_loop(node_id): # ficar rodando enquanto digitamos no terminal, while garante isso

    while True:

        command = input("> ") # durante o laco, fica esperando digitar

        if command == "exit":

            print(f"Encerrando Nó {node_id}")

            break

        elif command.startswith("send"): # caso mande um comando com inicio 'send'

            parts = command.split(maxsplit=2) # divide a string command em partes, maxsplit garante que mensagens tenham mais que um espaco

            if len(parts) < 3: # forca usuarioa enviar o comando da forma correta

                print("Uso correto: send <id_do_destino> <mensagem>")

                continue

            target_id = int(parts[1]) # segunda parte do parts é a nó destino

            message = parts[2] # terceira parte é a mensagem

            send_message(node_id, target_id, message)

        elif command == "request_cs":
            request_critical_section(node_id)

        elif command == "release_cs":
            release_critical_section(node_id)
        

        elif command == "leader":
            print(f"Líder atual conhecido pelo Nó {node_id}: Nó {leader_id}")

        elif command == "election":
            start_election(node_id)

        else:
            print("Comandos disponíveis:")
            print("send <id_do_destino> <mensagem>")
            print("request_cs")
            print("release_cs")
            print("leader")
            print("election")
            print("exit")

if __name__ == "__main__": # roda enquanto executamos o arquivo

    if len(sys.argv) != 2: # verifica se passou o id do nó 

        print("Uso correto: python node.py <id_do_no>")

        sys.exit(1)

    node_id = int(sys.argv[1]) # pega o id do nó


    # inicia o servidor em uma thread, fazendo o servidor rodar em paraelelo
    server_thread = threading.Thread(target=start_node, args=(node_id,), daemon=True)

    server_thread.start()

    command_loop(node_id) # inicia o termninal de comandos