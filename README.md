# Smart Locker — Protocolo MQTT e Integração ESP32/Software

Este documento descreve a comunicação MQTT atualmente implementada no protótipo do **Smart Locker**, com base nas versões finais do firmware do ESP32 (`main.cpp`) e do servidor de testes em Python (`server.py`).

O objetivo é permitir que a parte de software seja integrada ao controlador sem depender do menu de testes em Python. O `server.py` serve como **referência funcional do protocolo** e como ferramenta de diagnóstico, não como backend definitivo do projeto.

---

## 1. Visão geral da arquitetura

A comunicação atual possui três elementos principais:

```text
+-----------------------+
| Backend / server.py   |
| ou software definitivo|
+-----------+-----------+
            |
            | MQTT / TCP 1883
            v
+-----------------------+
| Broker Mosquitto      |
| 192.168.0.101         |
+-----------+-----------+
            |
            | Wi-Fi / MQTT
            v
+-----------------------+
| ESP32 Smart Locker    |
| controlador físico    |
+-----------------------+
```

No ambiente atual:

- Broker Mosquitto: `192.168.0.101:1883`
- O `server.py`, quando executado na mesma máquina do Mosquitto, usa `127.0.0.1:1883`.
- O ESP32 acessa o mesmo broker pela LAN usando `192.168.0.101:1883`.
- O ESP32 observado durante os testes recebeu o IP `192.168.0.100`.
- O identificador lógico atual do controlador nos tópicos é `1`.

Todos os payloads de aplicação são **binários**, não JSON.

---

## 2. Arquivos principais

### `main.cpp`

Firmware executado no ESP32.

Responsabilidades principais:

- conexão Wi-Fi;
- conexão e reconexão MQTT;
- recepção de comandos remotos;
- LINK e UNLINK de armários;
- abertura remota;
- persistência das configurações dos armários em NVS (`Preferences`);
- persistência dos eventos no LittleFS;
- sincronização de horário;
- envio de ACKs;
- publicação de estado;
- publicação de logs e alertas;
- controle físico das travas;
- leitura dos sensores pelo 74LS165;
- teclado e LCD;
- separação entre task de hardware e task de rede via FreeRTOS.

### `server.py`

Servidor de **teste e referência** em Python.

Responsabilidades:

- publicar comandos MQTT;
- montar corretamente os pacotes binários;
- aguardar e interpretar ACKs;
- receber logs;
- confirmar logs com `LOG_ACK`;
- responder solicitações de horário;
- receber alertas;
- receber e mostrar o estado dos armários;
- fornecer um menu de testes.

Dependência Python:

```bash
pip install paho-mqtt
```

---

# 3. Tópicos MQTT

Prefixo atual:

```text
smartlocker/controller/1/
```

| Tópico | Direção | Função | QoS |
|---|---|---|---:|
| `smartlocker/controller/1/cmd` | Software → ESP32 | Comandos para o controlador | 1 |
| `smartlocker/controller/1/ack` | ESP32 → Software | Confirmação de comandos | 1 |
| `smartlocker/controller/1/state` | ESP32 → Software | Estado de vínculo dos 8 armários | 1 |
| `smartlocker/controller/1/log` | ESP32 → Software | Eventos armazenados localmente | 1 |
| `smartlocker/controller/1/log/ack` | Software → ESP32 | Confirmação de recebimento de log | 1 |
| `smartlocker/controller/1/alert` | ESP32 → Software | Alertas em tempo real | 1 |
| `smartlocker/controller/1/time/request` | ESP32 → Software | Solicitação de horário | 1 |
| `smartlocker/controller/1/time/sync` | Software → ESP32 | Sincronização de horário | 1 |

O tópico `state` é publicado com **retain=true**. Isso significa que um cliente que se conectar depois pode receber imediatamente o último estado publicado pelo ESP32.

> Atenção: receber um `state` retido não prova, sozinho, que o ESP32 está online naquele instante.

---

# 4. Endianness e tipos

Os campos numéricos com mais de 1 byte utilizam **big-endian / network byte order**.

Exemplos:

```text
uint16 0x1234 -> 12 34
uint32 0x12345678 -> 12 34 56 78
```

No Python isso aparece usando `struct.pack()` com `!`.

Exemplo:

```python
struct.pack("!BBHH", comando, armario, usuario, senha)
```

---

# 5. Comandos

## 5.1 GET_LOGS — `0x01`

Solicita ao ESP32 o envio dos logs ainda não confirmados.

Pacote:

```text
Byte 0
+------+
| 0x01 |
+------+
```

Tamanho: **1 byte**

```text
01
```

O ESP32 não envia ACK de comando para `GET_LOGS`. Ele inicia o processo de transmissão dos logs pendentes.

Na V5.4 o envio automático de backlog está temporariamente desativado:

```cpp
constexpr bool ENVIO_LOG_AUTOMATICO = false;
```

Portanto, durante o diagnóstico atual, o envio dos logs é disparado manualmente por `GET_LOGS`.

---

## 5.2 LINK — `0x10`

Vincula um usuário e uma senha a um armário.

Formato:

```text
Byte 0   Byte 1   Bytes 2-3      Bytes 4-5
+------+---------+-------------+-------------+
| 0x10 | Locker  | User uint16 | Pass uint16 |
+------+---------+-------------+-------------+
```

Tamanho: **6 bytes**

Exemplo:

- armário: `3`
- usuário: `8888` = `0x22B8`
- senha: `1234` = `0x04D2`

Payload:

```text
10 03 22 B8 04 D2
```

Python:

```python
struct.pack("!BBHH", 0x10, armario, usuario, senha)
```

Regras atuais:

- `armario`: 1 a 8;
- `usuario`: 0 a 9999;
- `senha`: 0 a 9999;
- o software de teste exige entrada com exatamente 4 dígitos;
- o mesmo usuário não pode estar vinculado simultaneamente a dois armários diferentes.

Quando o LINK é aceito:

1. o ESP32 atualiza a configuração em RAM;
2. grava a configuração em NVS;
3. registra `ARMARIO_VINCULADO`;
4. envia ACK;
5. publica novamente o `state`.

A senha fica armazenada no ESP32 e **não é publicada no tópico `state`**.

---

## 5.3 UNLINK — `0x11`

Remove o vínculo de um armário.

Formato:

```text
Byte 0   Byte 1
+------+---------+
| 0x11 | Locker  |
+------+---------+
```

Tamanho: **2 bytes**

Exemplo — desvincular armário 3:

```text
11 03
```

Quando aceito, o ESP32:

- marca o armário como livre;
- zera usuário e senha;
- salva a alteração em NVS;
- registra `ARMARIO_DESVINCULADO`;
- envia ACK;
- publica novamente `state`.

---

## 5.4 OPEN_REMOTE — `0x12`

Solicita abertura remota de um armário.

Formato:

```text
Byte 0   Byte 1
+------+---------+
| 0x12 | Locker  |
+------+---------+
```

Tamanho: **2 bytes**

Exemplo — abrir armário 1:

```text
12 01
```

Fluxo atual:

```text
Software
   |
   | OPEN_REMOTE
   v
ESP32
   |
   | valida armário
   v
filaTravas
   |
   +----> ACK_OK enviado assim que a solicitação
   |      é aceita pela fila
   |
   v
Task de hardware
   |
   | seleciona 74LS138
   | DEC_ENABLE = HIGH
   v
trava liberada
   |
   | aproximadamente 1 s
   v
DEC_ENABLE = LOW
   |
   v
trava rearmada
```

**Importante:** `ACK_OK` de `OPEN_REMOTE` significa:

> “o ESP32 aceitou a solicitação e colocou o armário na fila de hardware”.

Ele **não é uma confirmação física** de que a porta abriu.

A confirmação física deve ser obtida pelos eventos/sensores, principalmente:

- `TRAVA_LIBERADA` (`0x04`);
- `TRAVA_REARMADA` (`0x05`);
- `PORTA_ABERTA` (`0x06`);
- `PORTA_FECHADA` (`0x07`).

O pulso atual da trava é:

```text
1000 ms
```

---

## 5.5 GET_STATE — `0x13`

Solicita o estado atual dos armários.

Pacote:

```text
13
```

Tamanho: **1 byte**

O ESP32 responde publicando `smartlocker/controller/1/state` e também envia um ACK de `GET_STATE`.

---

# 6. ACK de comando

Tópico:

```text
smartlocker/controller/1/ack
```

Formato:

```text
Byte 0       Byte 1       Byte 2
+-----------+------------+--------+
| Command   | Locker     | Status |
+-----------+------------+--------+
```

Tamanho: **3 bytes**

Exemplo:

```text
11 03 00
```

Significa:

```text
UNLINK
armário 3
SUCESSO
```

## Status

| Código | Nome | Significado |
|---:|---|---|
| `0x00` | `SUCESSO` | Comando aceito |
| `0x01` | `ARMARIO_INVALIDO` | Armário fora de 1–8 |
| `0x02` | `PARAMETRO_INVALIDO` | Dados inválidos ou conflito de usuário |
| `0x03` | `COMANDO_INVALIDO` | Código de comando desconhecido |
| `0x04` | `SISTEMA_OCUPADO_OU_FILA_CHEIA` | Recurso ocupado / fila cheia |

O `server.py` atualmente aguarda até:

```text
6 segundos
```

pelo ACK correspondente a LINK, UNLINK ou OPEN_REMOTE.

---

# 7. Estado dos armários — STATE

Tópico:

```text
smartlocker/controller/1/state
```

Publicado pelo ESP32 com:

```text
retain = true
QoS = 1
```

Formato:

```text
Byte 0 : 0x13
Byte 1 : bitmask de vínculos

Bytes 2-3   : usuário armário 1
Bytes 4-5   : usuário armário 2
Bytes 6-7   : usuário armário 3
Bytes 8-9   : usuário armário 4
Bytes 10-11 : usuário armário 5
Bytes 12-13 : usuário armário 6
Bytes 14-15 : usuário armário 7
Bytes 16-17 : usuário armário 8
```

Total:

```text
18 bytes
```

## Máscara

Cada bit representa um armário:

```text
bit 0 -> armário 1
bit 1 -> armário 2
bit 2 -> armário 3
...
bit 7 -> armário 8
```

Exemplo:

```text
mask = 0x07
```

Binário:

```text
00000111
```

Significa:

```text
armário 1 vinculado
armário 2 vinculado
armário 3 vinculado
armários 4-8 livres
```

A senha **não é enviada** no STATE.

---

# 8. Persistência dos vínculos

O ESP32 utiliza:

```cpp
Preferences preferences;
```

Namespace:

```text
lockers
```

Cada armário guarda:

```text
v -> vinculado
u -> usuário
p -> senha
```

Conceitualmente:

```text
l1v / l1u / l1p
l2v / l2u / l2p
...
l8v / l8u / l8p
```

Portanto, os vínculos sobrevivem a reboot e perda de alimentação.

---

# 9. Logs

Os eventos são primeiro armazenados localmente no ESP32.

Arquivo:

```text
/eventos.bin
```

Sistema de arquivos:

```text
LittleFS
```

Estrutura de cada registro:

```cpp
struct LogEvento {
    uint16_t id;
    uint32_t timestamp;
    uint8_t armario;
    uint8_t tipo;
};
```

Tamanho:

```text
8 bytes
```

## Payload MQTT do log

Tópico:

```text
smartlocker/controller/1/log
```

Formato:

```text
Bytes 0-1 : event_id uint16
Bytes 2-5 : timestamp Unix uint32
Byte 6    : armário
Byte 7    : tipo do evento
```

Tudo em big-endian.

Exemplo conceitual:

```text
02 26 6A DA 5D 91 03 0A
```

---

# 10. Tipos de evento

| Código | Evento |
|---:|---|
| `0x01` | `BOTAO_PRESSIONADO` |
| `0x02` | `SENHA_CORRETA` |
| `0x03` | `SENHA_INCORRETA` |
| `0x04` | `TRAVA_LIBERADA` |
| `0x05` | `TRAVA_REARMADA` |
| `0x06` | `PORTA_ABERTA` |
| `0x07` | `PORTA_FECHADA` |
| `0x08` | `PRESENCA_DETECTADA` |
| `0x09` | `METAL_DETECTADO` |
| `0x0A` | `ARMARIO_VINCULADO` |
| `0x0B` | `ARMARIO_DESVINCULADO` |
| `0x0C` | `ABERTURA_REMOTA` |
| `0x0D` | `CHAMADA_SISTEMA` |
| `0x0E` | `PORTA_ABERTA_TEMPO_EXCEDIDO` |
| `0x0F` | `HORARIO_ATUALIZADO` |

Quando `armario = 0`, o evento é considerado originado no controlador e não em um compartimento específico.

---

# 11. Confirmação dos logs — LOG_ACK

Depois que o software recebe um log válido, deve confirmar o recebimento.

Tópico:

```text
smartlocker/controller/1/log/ack
```

Formato:

```text
Bytes 0-1 : event_id uint16 big-endian
```

Exemplo:

```text
event_id = 550 decimal
550 = 0x0226

payload:
02 26
```

O ESP32 mantém:

```text
ultimoLogConfirmado
```

e também persiste esse valor em NVS.

Os logs com:

```text
event_id <= ultimoLogConfirmado
```

não precisam ser reenviados.

### Comportamento importante

Se o ESP32 não receber o `LOG_ACK`, ele pode retransmitir o mesmo evento.

O servidor deve ser **idempotente**:

1. receber o evento;
2. verificar `event_id`;
3. evitar duplicar o registro no banco;
4. enviar `LOG_ACK` mesmo se aquele evento já tiver sido recebido anteriormente.

O `server.py` de teste já segue esse comportamento.

### Limitação atual

O arquivo `/eventos.bin` é append-only no código atual. O ACK controla o que precisa ser reenviado, mas não existe ainda uma rotina definitiva de compactação/remoção física dos registros antigos.

---

# 12. Alertas em tempo real

Tópico:

```text
smartlocker/controller/1/alert
```

Atualmente existe:

```text
0x01 = PORTA_ABERTA_TEMPO_EXCEDIDO
```

Formato:

```text
Byte 0    : tipo do alerta
Byte 1    : armário
Bytes 2-5 : timestamp do alerta
Bytes 6-9 : timestamp de abertura da porta
```

Total:

```text
10 bytes
```

Atualmente o limite configurado para porta aberta é:

```text
30 segundos
```

---

# 13. Sincronização de horário

O ESP32 não depende do horário estar disponível imediatamente no boot.

Enquanto não houver sincronização:

```text
timestamp = 0
```

## TIME_REQUEST

ESP32 → software:

Tópico:

```text
smartlocker/controller/1/time/request
```

Payload:

```text
20
```

## TIME_SYNC

Software → ESP32:

Tópico:

```text
smartlocker/controller/1/time/sync
```

Formato:

```text
Byte 0    : 0x21
Bytes 1-4 : Unix timestamp uint32 big-endian
```

Total:

```text
5 bytes
```

Python:

```python
struct.pack("!BI", 0x21, int(time.time()))
```

O ESP32:

- solicita horário ao conectar ao MQTT;
- tenta novamente a cada 30 s enquanto ainda não estiver sincronizado;
- após sincronizar, solicita nova atualização aproximadamente a cada 6 h.

---

# 14. FreeRTOS / concorrência

O firmware utiliza duas tasks principais.

## HardwareTask

Executada no core 1, prioridade 2.

Responsável por:

- teclado;
- botão;
- fila das travas;
- sensores;
- LCD;
- comandos locais via Serial.

## NetworkTask

Executada no core 0, prioridade 1.

Responsável por:

- Wi-Fi;
- MQTT;
- callbacks;
- comandos MQTT;
- sincronização de horário;
- alertas;
- envio de logs.

A ideia é que uma queda de rede não bloqueie teclado, sensores ou acionamento local.

---

# 15. Recepção de comandos no ESP32

O callback MQTT não executa diretamente toda a lógica do comando.

Fluxo:

```text
MQTT callback
    |
    | copia payload
    v
comandoPendente[]
    |
    | temComandoPendente = true
    v
NetworkTask
    |
    v
processarComandoPendente()
```

Isso evita executar operações mais pesadas diretamente dentro do callback MQTT.

### Limitação importante do protocolo atual

Existe apenas:

```cpp
uint8_t comandoPendente[16];
bool temComandoPendente;
```

Portanto, a implementação atual deve ser tratada como **um comando pendente por vez**.

O software deve seguir:

```text
enviar comando
      ↓
aguardar ACK ou timeout
      ↓
somente depois enviar o próximo comando
```

Não enviar vários LINK/UNLINK/OPEN_REMOTE em paralelo.

Além disso, o ACK atual não possui `request_id`/`transaction_id`. A correlação é feita por:

```text
comando + armário
```

Para uma versão de produção, é recomendável acrescentar um identificador de transação.

---

# 16. Filas de trava

O ESP32 possui:

```text
filaTravas
```

com capacidade para até 8 solicitações.

Somente uma trava recebe o pulso elétrico por vez.

Isso ocorre porque o hardware utiliza o 74LS138 para selecionar uma saída.

Fluxo:

```text
fila
 ↓
selecionar armário
 ↓
A/B/C do 74LS138
 ↓
DEC_ENABLE
 ↓
pulso de aproximadamente 1 segundo
 ↓
desabilita decoder
 ↓
processa próximo item
```

Várias portas podem permanecer fisicamente abertas ao mesmo tempo; o que é serializado é apenas o pulso elétrico de abertura.

---

# 17. Reconexão

Configuração atual:

```text
INTERVALO_RECONEXAO_MS = 60000
```

Ou seja, quando offline, uma nova tentativa ocorre aproximadamente a cada 1 minuto.

O firmware imprime periodicamente:

```text
[NET STATUS] WiFi=OK IP=... MQTT=OK
```

O client ID MQTT atual é fixo:

```text
esp32-smartlocker-1
```

### Importante para integração

Não podem existir dois clientes MQTT conectando ao mesmo broker com o mesmo client ID.

Se outro dispositivo/software utilizar:

```text
esp32-smartlocker-1
```

o broker pode derrubar a sessão anterior.

Para múltiplos controladores, utilizar IDs únicos, por exemplo:

```text
esp32-smartlocker-1
esp32-smartlocker-2
esp32-smartlocker-3
```

e parametrizar também o número do controlador nos tópicos.

---

# 18. Escalabilidade dos tópicos

Hoje está fixo:

```text
smartlocker/controller/1/...
```

Para múltiplos controladores, a estrutura recomendada é:

```text
smartlocker/controller/{controller_id}/cmd
smartlocker/controller/{controller_id}/ack
smartlocker/controller/{controller_id}/state
smartlocker/controller/{controller_id}/log
smartlocker/controller/{controller_id}/log/ack
smartlocker/controller/{controller_id}/alert
smartlocker/controller/{controller_id}/time/request
smartlocker/controller/{controller_id}/time/sync
```

Exemplo:

```text
smartlocker/controller/2/cmd
```

para o controlador 2.

---

# 19. Segurança

A versão atual é um protótipo acadêmico.

Atualmente:

- MQTT usa porta `1883`;
- não existe TLS;
- não existe autenticação MQTT no código mostrado;
- SSID e senha Wi-Fi estão definidos diretamente no firmware;
- usuário/senha de armário trafegam no pacote LINK sem criptografia de aplicação.

Antes de uso em produção:

- não versionar credenciais no repositório;
- mover configurações sensíveis para arquivo de configuração/secrets;
- utilizar autenticação no broker;
- avaliar MQTT sobre TLS;
- implementar autorização por controlador;
- evitar transportar senha do armário em texto binário simples;
- implementar rotação/provisionamento seguro de credenciais.

---

# 20. Como executar o servidor de teste

Exemplo:

```bash
python3 -m venv venv
source venv/bin/activate
pip install paho-mqtt
python server_smartlocker_v5_4.py
```

O broker local deve estar disponível em:

```text
127.0.0.1:1883
```

e acessível pelo ESP32 em:

```text
192.168.0.101:1883
```

---

# 21. Ferramentas úteis de diagnóstico

## Observar todos os tópicos

```bash
mosquitto_sub \
  -h 192.168.0.101 \
  -p 1883 \
  -t 'smartlocker/controller/1/#' \
  -v
```

## Testar GET_STATE diretamente

```bash
printf '\x13' | mosquitto_pub \
  -h 192.168.0.101 \
  -p 1883 \
  -t 'smartlocker/controller/1/cmd' \
  -q 1 \
  -s
```

Serial esperado:

```text
[MQTT RX] topic=smartlocker/controller/1/cmd len=1
[CMD RX] len=1 HEX=13
[CMD] Processando 0x13 len=1
[STATE TX] ...
[ACK TX] cmd=0x13 armario=0 status=0x00 -> OK
```

## Testar UNLINK do armário 3 diretamente

```bash
printf '\x11\x03' | mosquitto_pub \
  -h 192.168.0.101 \
  -p 1883 \
  -t 'smartlocker/controller/1/cmd' \
  -q 1 \
  -s
```

## Ver payload binário

```bash
mosquitto_sub \
  -h 192.168.0.101 \
  -p 1883 \
  -t 'smartlocker/controller/1/cmd' \
  -C 1 | xxd -g 1
```

## Logs do Mosquitto

```bash
sudo journalctl -u mosquitto -f
```

Útil para investigar:

- desconexões;
- reconexões;
- client ID duplicado;
- erros do broker.

---

# 22. Estado atual / pontos ainda em diagnóstico

A V5.4 foi criada também para diagnosticar a comunicação MQTT.

Já foi confirmado nos testes:

```text
ESP32 -> broker -> servidor
```

funciona para:

- STATE;
- TIME_REQUEST;
- LOG.

Também foi confirmado manualmente:

```text
broker -> ESP32
```

com `GET_STATE (0x13)`:

```text
[MQTT SUB] CMD=OK | TIME_SYNC=OK | LOG_ACK=OK
[MQTT RX] topic=smartlocker/controller/1/cmd len=1
[CMD RX] len=1 HEX=13
[CMD] Processando 0x13 len=1
[STATE TX] mask=0x07 -> OK
[ACK TX] cmd=0x13 armario=0 status=0x00 -> OK
```

Entretanto, durante os testes também foram observadas **conexões e desconexões MQTT intermitentes** e alguns comandos enviados pelo `server.py` resultaram em timeout.

Essa causa ainda deve ser considerada **em diagnóstico**, não resolvida.

Pontos a verificar:

1. log do Mosquitto durante a desconexão;
2. possível uso duplicado do client ID `esp32-smartlocker-1`;
3. estabilidade da sessão MQTT;
4. se o pacote `cmd` aparece no Serial do ESP32 quando enviado pelo `server.py`;
5. recebimento dos `LOG_ACK`;
6. se existe alguma nova condição que mantenha a `NetworkTask` ocupada.

---

# 23. Contrato recomendado para o backend

Para integrar o backend definitivo, seguir este comportamento:

### LINK

```text
publish cmd: 10 LL UU UU PP PP
wait ack
if ACK_OK:
    wait/update STATE
```

### UNLINK

```text
publish cmd: 11 LL
wait ack
if ACK_OK:
    wait/update STATE
```

### OPEN_REMOTE

```text
publish cmd: 12 LL
wait ack

ACK_OK = solicitação aceita
```

Para confirmar fisicamente:

```text
aguardar eventos TRAVA_LIBERADA / PORTA_ABERTA
```

### LOG

Ao receber:

```text
[event_id][timestamp][locker][event]
```

o backend deve:

```text
1. salvar de maneira idempotente;
2. responder LOG_ACK(event_id);
```

### STATE

O backend deve tratar `state` como a visão atual de vínculo do controlador.

Não armazenar nem esperar senha nesse pacote.

---

# 24. Resumo rápido do protocolo

```text
COMANDOS
01                         GET_LOGS
10 LL UU UU PP PP          LINK
11 LL                      UNLINK
12 LL                      OPEN_REMOTE
13                         GET_STATE

ACK
CC LL SS                   command / locker / status

TIME
20                         TIME_REQUEST
21 TT TT TT TT             TIME_SYNC

LOG
II II TT TT TT TT LL EE    8 bytes

LOG_ACK
II II                      2 bytes

STATE
13 MM U1 U1 ... U8 U8      18 bytes

ALERT
AA LL TT TT TT TT OO OO OO OO
```

Legenda:

```text
CC = comando
LL = armário
SS = status
UU = usuário uint16 BE
PP = senha uint16 BE
II = event ID uint16 BE
TT = timestamp uint32 BE
EE = evento
MM = máscara de vínculos
AA = tipo de alerta
OO = timestamp de abertura
```

---

## Observação final

O protocolo atual foi criado para um protótipo de 8 compartimentos e prioriza mensagens compactas, persistência local e funcionamento do hardware mesmo sem conexão com o servidor.

Para a integração de software, os pontos mais importantes são:

1. tratar os payloads como **binários**;
2. usar **big-endian**;
3. enviar **um comando por vez** e aguardar ACK;
4. considerar `OPEN_REMOTE ACK_OK` como **aceitação da solicitação**, não confirmação física;
5. processar logs de forma idempotente;
6. sempre responder `LOG_ACK`;
7. usar `STATE` para sincronizar os vínculos;
8. não depender do servidor estar online para o funcionamento local do armário;
9. usar client IDs únicos no MQTT;
10. parametrizar `controller_id` quando o sistema passar de um controlador.
