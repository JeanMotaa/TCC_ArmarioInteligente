import struct
import time
import threading
from datetime import datetime

import paho.mqtt.client as mqtt

# ============================================================
# CONFIGURACAO
# ============================================================
BROKER = "127.0.0.1"
PORT = 1883

TOPIC_CMD = "smartlocker/controller/1/cmd"
TOPIC_LOG = "smartlocker/controller/1/log"
TOPIC_ACK = "smartlocker/controller/1/ack"
TOPIC_LOG_ACK = "smartlocker/controller/1/log/ack"
TOPIC_ALERT = "smartlocker/controller/1/alert"
TOPIC_TIME_REQUEST = "smartlocker/controller/1/time/request"
TOPIC_TIME_SYNC = "smartlocker/controller/1/time/sync"
TOPIC_STATE = "smartlocker/controller/1/state"

# ============================================================
# PROTOCOLO
# ============================================================
CMD_GET_LOGS = 0x01
CMD_LINK = 0x10
CMD_UNLINK = 0x11
CMD_OPEN_REMOTE = 0x12
CMD_GET_STATE = 0x13
CMD_TIME_REQUEST = 0x20
CMD_TIME_SYNC = 0x21

STATUS = {
    0x00: "SUCESSO",
    0x01: "ARMARIO_INVALIDO",
    0x02: "PARAMETRO_INVALIDO",
    0x03: "COMANDO_INVALIDO",
    0x04: "SISTEMA_OCUPADO_OU_FILA_CHEIA",
}

EVENTOS = {
    0x01: "BOTAO_PRESSIONADO",
    0x02: "SENHA_CORRETA",
    0x03: "SENHA_INCORRETA",
    0x04: "TRAVA_LIBERADA",
    0x05: "TRAVA_REARMADA",
    0x06: "PORTA_ABERTA",
    0x07: "PORTA_FECHADA",
    0x08: "PRESENCA_DETECTADA",
    0x09: "METAL_DETECTADO",
    0x0A: "ARMARIO_VINCULADO",
    0x0B: "ARMARIO_DESVINCULADO",
    0x0C: "ABERTURA_REMOTA",
    0x0D: "CHAMADA_SISTEMA",
    0x0E: "PORTA_ABERTA_TEMPO_EXCEDIDO",
    0x0F: "HORARIO_ATUALIZADO",
}

ALERTAS = {
    0x01: "PORTA_ABERTA_TEMPO_EXCEDIDO",
}

# Evita imprimir duplicatas durante esta execucao do servidor.
logs_recebidos = set()

# Historico dos logs recebidos nesta execucao do servidor.
# E apenas em memoria: ao reiniciar o server.py, esta lista zera.
historico_logs = []

# Reduz poluicao visual por retransmissoes durante diagnostico.
duplicatas_log_contagem = {}


# Estado sincronizado com o ESP32.
# Cada item guarda apenas informacao de vinculo e usuario; senha nunca sai do ESP32.
estado_armarios = {
    armario: {"vinculado": False, "usuario": 0}
    for armario in range(1, 9)
}
estado_armarios_valido = False

# Sincronizacao entre a thread MQTT (callbacks) e a thread do menu.
cond_ack = threading.Condition()
ultimo_ack = None

cond_estado = threading.Condition()
evento_mqtt_conectado = threading.Event()

TIMEOUT_ACK_COMANDO_S = 6.0
TIMEOUT_ESTADO_S = 3.0

# ============================================================
# AUXILIARES
# ============================================================
def nome_comando(codigo):
    nomes = {
        CMD_GET_LOGS: "GET_LOGS",
        CMD_LINK: "LINK",
        CMD_UNLINK: "UNLINK",
        CMD_OPEN_REMOTE: "OPEN_REMOTE",
        CMD_GET_STATE: "GET_STATE",
        CMD_TIME_REQUEST: "TIME_REQUEST",
        CMD_TIME_SYNC: "TIME_SYNC",
    }
    return nomes.get(codigo, f"DESCONHECIDO_0x{codigo:02X}")


def validar_armario(texto):
    try:
        armario = int(texto)
    except ValueError:
        return None
    return armario if 1 <= armario <= 8 else None


def validar_4_digitos(valor):
    return len(valor) == 4 and valor.isdigit()


def formatar_timestamp(timestamp):
    if timestamp == 0:
        return "NAO_SINCRONIZADO"
    try:
        return datetime.fromtimestamp(timestamp).astimezone().isoformat(timespec="seconds")
    except (OverflowError, OSError, ValueError):
        return f"INVALIDO({timestamp})"

# ============================================================
# HORARIO
# ============================================================
def enviar_horario(client):
    timestamp = int(time.time())
    pacote = struct.pack("!BI", CMD_TIME_SYNC, timestamp)

    # IMPORTANTE:
    # Esta funcao pode ser chamada a partir de on_message().
    # Nunca usar wait_for_publish() dentro de callback MQTT, pois o callback
    # roda na propria thread de rede do Paho e isso pode causar deadlock.
    resultado = client.publish(TOPIC_TIME_SYNC, payload=pacote, qos=1)

    if resultado.rc != mqtt.MQTT_ERR_SUCCESS:
        print("[TIME] Falha ao enfileirar TIME_SYNC. rc=", resultado.rc)
        return False

    print()
    print("======= TIME_SYNC =======")
    print("Timestamp:", timestamp)
    print("Horario:", formatar_timestamp(timestamp))
    print("HEX:", pacote.hex(" "))
    print("=========================")
    return True

# ============================================================
# ACK DE LOG
# ============================================================
def enviar_log_ack(client, event_id):
    pacote = struct.pack("!H", event_id)
    client.publish(TOPIC_LOG_ACK, payload=pacote, qos=1)

# ============================================================
# MQTT
# ============================================================
def on_connect(client, userdata, flags, reason_code, properties):
    print()
    if reason_code.is_failure:
        print("[MQTT] Erro ao conectar:", reason_code)
        evento_mqtt_conectado.clear()
        return

    print("[MQTT] Conectado ao broker.")
    for topico in [
        TOPIC_LOG,
        TOPIC_ACK,
        TOPIC_ALERT,
        TOPIC_TIME_REQUEST,
        TOPIC_STATE,
    ]:
        client.subscribe(topico, qos=1)
        print("[MQTT] Inscrito em:", topico)

    evento_mqtt_conectado.set()

    # Pede o estado atual logo ao conectar. O ESP32 tambem publica STATE retido.
    client.publish(TOPIC_CMD, payload=bytes([CMD_GET_STATE]), qos=1)


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    evento_mqtt_conectado.clear()
    print()
    print("[MQTT] Desconectado do broker:", reason_code)


# ============================================================
# ACK DE COMANDO
# ============================================================
def processar_ack(dados):
    global ultimo_ack

    if len(dados) != 3:
        print(f"[ERRO] ACK deveria possuir 3 bytes. Recebidos: {len(dados)}")
        return

    comando, armario, status = dados
    ack = {
        "comando": comando,
        "armario": armario,
        "status": status,
        "recebido_em": time.monotonic(),
    }

    with cond_ack:
        ultimo_ack = ack
        cond_ack.notify_all()

    print()
    print("========== ACK RX ========")
    print("Comando:", nome_comando(comando))
    print("Armario:", armario)
    print("Status:", STATUS.get(status, f"DESCONHECIDO_{status}"))
    print("HEX:", dados.hex(" "))
    print("==========================")


# ============================================================
# LOG
# ============================================================
def processar_log(client, dados):
    if len(dados) != 8:
        print(f"[ERRO] Log deveria possuir 8 bytes. Recebidos: {len(dados)}")
        return

    event_id, timestamp, armario, tipo = struct.unpack("!HIBB", dados)

    # Confirma sempre um pacote valido, inclusive retransmissoes.
    enviar_log_ack(client, event_id)

    if event_id in logs_recebidos:
        duplicatas_log_contagem[event_id] = duplicatas_log_contagem.get(event_id, 0) + 1
        qtd = duplicatas_log_contagem[event_id]
        if qtd <= 3 or qtd % 10 == 0:
            print(f"[LOG] ID {event_id} retransmitido {qtd}x; ACK reenviado.")
        return

    logs_recebidos.add(event_id)
    evento = EVENTOS.get(tipo, f"DESCONHECIDO_0x{tipo:02X}")

    historico_logs.append({
        "id": event_id,
        "timestamp": timestamp,
        "armario": armario,
        "tipo": tipo,
        "evento": evento,
    })

    print()
    print("========== LOG ==========")
    print("ID:", event_id)
    print("Timestamp:", timestamp)
    print("Horario:", formatar_timestamp(timestamp))
    if armario == 0:
        print("Origem: CONTROLADOR")
    else:
        print("Armario:", armario)
    print("Evento:", evento)
    print("HEX:", dados.hex(" "))
    print("=========================")

# ============================================================
# ALERTA
# ============================================================
def processar_alerta(dados):
    if len(dados) != 10:
        print(f"[ERRO] ALERT deveria possuir 10 bytes. Recebidos: {len(dados)}")
        return

    tipo, armario, timestamp_alerta, timestamp_abertura = struct.unpack("!BBII", dados)
    nome = ALERTAS.get(tipo, f"DESCONHECIDO_0x{tipo:02X}")

    print()
    print("========= ALERTA =========")
    print("Tipo:", nome)
    print("Armario:", armario)
    print("Porta aberta desde:", formatar_timestamp(timestamp_abertura))
    print("Alerta gerado em:", formatar_timestamp(timestamp_alerta))
    if timestamp_abertura > 0 and timestamp_alerta >= timestamp_abertura:
        print("Tempo aberta:", timestamp_alerta - timestamp_abertura, "segundos")
    print("HEX:", dados.hex(" "))
    print("==========================")

# ============================================================
# TIME_REQUEST
# ============================================================
def processar_time_request(client, dados):
    if len(dados) != 1 or dados[0] != CMD_TIME_REQUEST:
        print("[TIME] TIME_REQUEST invalido.")
        return

    print("[TIME] ESP32 solicitou horario.")
    if enviar_horario(client):
        print("[TIME] TIME_SYNC enfileirado sem bloquear callback MQTT.")

# ============================================================
# ESTADO DOS ARMARIOS
# Pacote: [0x13][mask][user1_hi][user1_lo]...[user8_hi][user8_lo]
# ============================================================
def processar_estado(dados):
    global estado_armarios_valido

    if len(dados) != 18 or dados[0] != CMD_GET_STATE:
        print(f"[ERRO] STATE invalido. Tamanho={len(dados)} HEX={dados.hex(' ')}")
        return

    mascara = dados[1]
    usuarios = struct.unpack("!8H", dados[2:18])

    with cond_estado:
        for armario in range(1, 9):
            vinculado = bool(mascara & (1 << (armario - 1)))
            estado_armarios[armario]["vinculado"] = vinculado
            estado_armarios[armario]["usuario"] = usuarios[armario - 1] if vinculado else 0
        estado_armarios_valido = True
        cond_estado.notify_all()

    vinculados = [
        str(a) for a in range(1, 9)
        if estado_armarios[a]["vinculado"]
    ]
    print("[STATE] Vinculados:", ", ".join(vinculados) if vinculados else "nenhum")


def resumo_armarios():
    if not estado_armarios_valido:
        return "ARMARIOS: estado ainda nao sincronizado"

    itens = []
    for armario in range(1, 9):
        info = estado_armarios[armario]
        if info["vinculado"]:
            itens.append(f"{armario}(usr {info['usuario']:04d})")

    if not itens:
        return "ARMARIOS VINCULADOS: nenhum"
    return "ARMARIOS VINCULADOS: " + ", ".join(itens)


def solicitar_estado(client, aguardar=False):
    global estado_armarios_valido

    if not client.is_connected():
        print("[ERRO] MQTT desconectado; nao foi possivel consultar o estado.")
        return False

    if aguardar:
        with cond_estado:
            estado_armarios_valido = False

        info = client.publish(TOPIC_CMD, payload=bytes([CMD_GET_STATE]), qos=1)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            print(f"[ERRO] Falha ao solicitar STATE. rc={info.rc}")
            return False

        limite = time.monotonic() + TIMEOUT_ESTADO_S
        with cond_estado:
            while not estado_armarios_valido:
                restante = limite - time.monotonic()
                if restante <= 0:
                    break
                cond_estado.wait(timeout=restante)
            return estado_armarios_valido

    client.publish(TOPIC_CMD, payload=bytes([CMD_GET_STATE]), qos=1)
    return True


# ============================================================
# CALLBACK
# ============================================================
def on_message(client, userdata, msg):
    dados = msg.payload

    if msg.topic == TOPIC_ACK:
        processar_ack(dados)
    elif msg.topic == TOPIC_LOG:
        processar_log(client, dados)
    elif msg.topic == TOPIC_ALERT:
        processar_alerta(dados)
    elif msg.topic == TOPIC_TIME_REQUEST:
        processar_time_request(client, dados)
    elif msg.topic == TOPIC_STATE:
        processar_estado(dados)
    else:
        print("[MQTT] Topico desconhecido:", msg.topic)

# ============================================================
# PUBLICAR COMANDO / AGUARDAR ACK
# ============================================================
def publicar_comando(client, pacote):
    if not client.is_connected():
        raise RuntimeError("MQTT desconectado")

    resultado = client.publish(TOPIC_CMD, payload=pacote, qos=1)
    if resultado.rc != mqtt.MQTT_ERR_SUCCESS:
        raise RuntimeError(f"Falha ao enfileirar publish MQTT. rc={resultado.rc}")
    return resultado


def enviar_comando_aguardando_ack(client, pacote, comando, armario, timeout=TIMEOUT_ACK_COMANDO_S):
    global ultimo_ack

    if not client.is_connected():
        print("[ERRO] MQTT desconectado. Comando nao enviado.")
        return None

    # Limpa ACK antigo antes de publicar.
    with cond_ack:
        ultimo_ack = None

    resultado = client.publish(TOPIC_CMD, payload=pacote, qos=1)
    if resultado.rc != mqtt.MQTT_ERR_SUCCESS:
        print(f"[ERRO] Falha ao enfileirar comando MQTT. rc={resultado.rc}")
        return None

    print(f"[MQTT TX] {nome_comando(comando)} armario={armario} enfileirado no broker")

    # O timeout abaixo é independente do PUBACK do broker.
    # Esperamos apenas o ACK da aplicacao enviado pela ESP32.
    limite = time.monotonic() + timeout

    with cond_ack:
        while True:
            if ultimo_ack is not None:
                mesmo_comando = ultimo_ack["comando"] == comando
                mesmo_armario = (
                    ultimo_ack["armario"] == armario
                    or ultimo_ack["armario"] == 0
                )
                if mesmo_comando and mesmo_armario:
                    return dict(ultimo_ack)

            restante = limite - time.monotonic()
            if restante <= 0:
                break

            cond_ack.wait(timeout=restante)

    print()
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print("[ERRO] TIMEOUT: nenhum ACK correspondente")
    print("Comando:", nome_comando(comando))
    print("Armario:", armario)
    print(f"Tempo aguardado: {timeout:.1f} s")
    print("Verifique Serial do ESP32, MQTT e broker.")
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    return None


def mostrar_resultado_ack(ack, acao):
    if ack is None:
        print(f"[FALHA] {acao}: ESP32 nao confirmou o comando.")
        return False

    status = ack["status"]
    nome_status = STATUS.get(status, f"DESCONHECIDO_{status}")

    if status == 0x00:
        print(f"[OK] {acao}: {nome_status}")
        return True

    print(f"[FALHA] {acao}: {nome_status}")
    if status == 0x01:
        print("Motivo: ID de armario invalido.")
    elif status == 0x02:
        print("Motivo: parametro invalido ou usuario ja utilizado em outro armario.")
    elif status == 0x03:
        print("Motivo: comando nao reconhecido pelo ESP32.")
    elif status == 0x04:
        print("Motivo: sistema ocupado ou fila de travas cheia.")
    return False


# ============================================================
# LINK
# ============================================================
def linkar_armario(client):
    print("\n--- LINKAR ARMARIO ---")
    armario = validar_armario(input("ID do armario [1-8]: ").strip())
    if armario is None:
        print("Armario invalido. Use um ID entre 1 e 8.")
        return

    usuario = input("Usuario (4 digitos): ").strip()
    if not validar_4_digitos(usuario):
        print("Usuario deve possuir exatamente 4 digitos numericos.")
        return

    senha = input("Senha (4 digitos): ").strip()
    if not validar_4_digitos(senha):
        print("Senha deve possuir exatamente 4 digitos numericos.")
        return

    pacote = struct.pack("!BBHH", CMD_LINK, armario, int(usuario), int(senha))
    print("LINK enviado. Armario:", armario, "Usuario:", usuario)
    print("HEX:", pacote.hex(" "))
    print("Aguardando ACK real da ESP32...")

    ack = enviar_comando_aguardando_ack(client, pacote, CMD_LINK, armario)
    if mostrar_resultado_ack(ack, f"LINK armario {armario}"):
        solicitar_estado(client, aguardar=True)
        print(resumo_armarios())


# ============================================================
# UNLINK
# ============================================================
def desvincular_armario(client):
    print("\n--- DESVINCULAR ARMARIO ---")
    armario = validar_armario(input("ID do armario [1-8]: ").strip())
    if armario is None:
        print("Armario invalido. Use um ID entre 1 e 8.")
        return

    pacote = struct.pack("!BB", CMD_UNLINK, armario)
    print("UNLINK enviado para armario", armario)
    print("Aguardando ACK real da ESP32...")

    ack = enviar_comando_aguardando_ack(client, pacote, CMD_UNLINK, armario)
    if mostrar_resultado_ack(ack, f"UNLINK armario {armario}"):
        solicitar_estado(client, aguardar=True)
        print(resumo_armarios())


# ============================================================
# OPEN_REMOTE
# ============================================================
def abrir_armario(client):
    print("\n--- ABERTURA REMOTA ---")
    armario = validar_armario(input("ID do armario [1-8]: ").strip())
    if armario is None:
        print("Armario invalido. Use um ID entre 1 e 8.")
        return

    pacote = struct.pack("!BB", CMD_OPEN_REMOTE, armario)
    print("OPEN_REMOTE enviado para armario", armario)
    print("Aguardando ACK real da ESP32...")

    ack = enviar_comando_aguardando_ack(client, pacote, CMD_OPEN_REMOTE, armario)
    if mostrar_resultado_ack(ack, f"OPEN_REMOTE armario {armario}"):
        print("Comando ACEITO pela ESP32 e colocado na fila de hardware.")
        print("Confirme no Serial: [TRAVA] Liberada armario X.")


# ============================================================
# GET_LOGS
# ============================================================
def solicitar_logs(client):
    pacote = bytes([CMD_GET_LOGS])
    try:
        publicar_comando(client, pacote)
    except RuntimeError as erro:
        print("[ERRO]", erro)
        return
    print("GET_LOGS enviado. A ESP32 reenviara os logs pendentes.")

# ============================================================
# VISUALIZAR LOGS RECEBIDOS
# ============================================================
def mostrar_logs_recebidos():
    print()
    print("===============================================")
    print("     LOGS RECEBIDOS NESTA EXECUCAO")
    print("===============================================")

    if not historico_logs:
        print("Nenhum log recebido ate o momento.")
        print("===============================================")
        return

    print(f"Total: {len(historico_logs)} evento(s)")
    print()

    for item in historico_logs:
        origem = (
            "CONTROLADOR"
            if item["armario"] == 0
            else f'ARMARIO {item["armario"]}'
        )

        print(
            f'ID {item["id"]:5d} | '
            f'{formatar_timestamp(item["timestamp"])} | '
            f'{origem:<12} | '
            f'{item["evento"]}'
        )

    print("===============================================")


# ============================================================
# MENU
# ============================================================
def menu():
    print()
    print("==============================================")
    print("          SMART LOCKER TEST V5.4")
    print("==============================================")
    print(resumo_armarios())
    print("----------------------------------------------")
    print("1 - Linkar armario")
    print("2 - Desvincular armario")
    print("3 - Abrir armario remotamente")
    print("4 - Solicitar logs pendentes")
    print("5 - Enviar horario agora")
    print("6 - Ver log atual do servidor")
    print("7 - Atualizar estado dos armarios")
    print("8 - Sair")
    print("==============================================")


# ============================================================
# MAIN
# ============================================================
client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id="smartlocker-test-server"
)
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

print()
print("==================================")
print("      SERVIDOR SMART LOCKER V5.4")
print("==================================")
print("Broker:", BROKER)

client.connect(BROKER, PORT, 60)
client.loop_start()

if not evento_mqtt_conectado.wait(5.0):
    print("[ERRO] Nao foi possivel confirmar conexao MQTT em 5 segundos.")
else:
    # Aguarda o primeiro STATE retido/resposta ao GET_STATE.
    with cond_estado:
        if not estado_armarios_valido:
            cond_estado.wait(TIMEOUT_ESTADO_S)

try:
    while True:
        menu()
        opcao = input("Escolha: ").strip()

        if opcao == "1":
            linkar_armario(client)
        elif opcao == "2":
            desvincular_armario(client)
        elif opcao == "3":
            abrir_armario(client)
        elif opcao == "4":
            solicitar_logs(client)
        elif opcao == "5":
            enviar_horario(client)
        elif opcao == "6":
            mostrar_logs_recebidos()
        elif opcao == "7":
            if solicitar_estado(client, aguardar=True):
                print(resumo_armarios())
            else:
                print("[ERRO] Nao foi possivel atualizar o estado.")
        elif opcao == "8":
            break
        else:
            print("Opcao invalida.")

except KeyboardInterrupt:
    print("\nServidor interrompido.")

finally:
    client.loop_stop()
    client.disconnect()
    print("Servidor encerrado.")
