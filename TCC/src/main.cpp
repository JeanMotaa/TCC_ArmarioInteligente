
#include <Arduino.h>
#include <WiFi.h>
#include <MQTT.h>
#include <Preferences.h>
#include <LittleFS.h>
#include <Wire.h>
#include <LiquidCrystal_I2C.h>
#include <Keypad.h>
#include <time.h>
#include <sys/time.h>
#include <freertos/FreeRTOS.h>
#include <freertos/task.h>
#include <freertos/queue.h>
#include <freertos/semphr.h>

// ============================================================
// SMART LOCKER - V5.4
// Hardware e rede em tasks separadas.
// Logs sempre salvos localmente com timestamp.
// Envio normal dos logs: a cada 15 min.
// Reconexao Wi-Fi/MQTT: a cada 1 min quando offline.
// ============================================================

// -------------------- REDE --------------------
const char WIFI_SSID[] = "SMART_LOCKER_TESTE";
const char WIFI_PASSWORD[] = "24664684";
const char MQTT_SERVER[] = "192.168.0.101"; // confirme com hostname -I
const uint16_t MQTT_PORT = 1883;

// -------------------- TEMPOS --------------------
constexpr uint8_t NUM_ARMARIOS = 8;
constexpr unsigned long TEMPO_PULSO_TRAVA_MS = 1000;
constexpr unsigned long TEMPO_ALERTA_PORTA_MS = 30000;
constexpr unsigned long DEBOUNCE_SENSOR_MS = 150;
constexpr unsigned long DEBOUNCE_BOTAO_MS = 50;
constexpr unsigned long INTERVALO_ENVIO_LOG_MS = 15UL * 60UL * 1000UL;
constexpr unsigned long INTERVALO_RECONEXAO_MS = 60UL * 1000UL;
constexpr unsigned long INTERVALO_SYNC_HORA_MS = 6UL * 60UL * 60UL * 1000UL;
constexpr unsigned long INTERVALO_RETRY_HORA_MS = 30UL * 1000UL;
constexpr unsigned long TIMEOUT_ACK_LOG_MS = 2000;
constexpr size_t TAMANHO_LOTE_LOG = 32;

// Diagnostico/demo: não sincroniza backlog automaticamente.
// GET_LOGS manual continua funcionando.
constexpr bool ENVIO_LOG_AUTOMATICO = false;

// -------------------- LCD --------------------
constexpr uint8_t SDA_PIN = 18;
constexpr uint8_t SCL_PIN = 19;
LiquidCrystal_I2C lcd(0x27, 16, 2);

// -------------------- TECLADO --------------------
const byte LINHAS = 4;
const byte COLUNAS = 3;
char teclas[LINHAS][COLUNAS] = {
    {'1','2','3'},
    {'4','5','6'},
    {'7','8','9'},
    {'*','0','#'}
};
byte pinosLinhas[LINHAS] = {13, 14, 16, 17};
byte pinosColunas[COLUNAS] = {26, 27, 32};
Keypad teclado = Keypad(makeKeymap(teclas), pinosLinhas, pinosColunas, LINHAS, COLUNAS);

// -------------------- 74LS138 / TRAVAS --------------------
constexpr uint8_t DEC_A = 21;
constexpr uint8_t DEC_B = 22;
constexpr uint8_t DEC_C = 23;
constexpr uint8_t DEC_ENABLE = 33;

// -------------------- 74LS165 / SENSORES --------------------
constexpr uint8_t SR_LOAD = 25;
constexpr uint8_t SR_CLOCK = 4;
constexpr uint8_t SR_DATA = 34;

// -------------------- BOTAO --------------------
// GPIO35 exige pull-up externo, ex.: 5k para 3V3.
constexpr uint8_t BOTAO_PIN = 35;
bool ultimoEstadoBotao = HIGH;
bool estadoEstavelBotao = HIGH;
unsigned long instanteMudancaBotao = 0;

// -------------------- MQTT --------------------
const char TOPIC_CMD[] = "smartlocker/controller/1/cmd";
const char TOPIC_LOG[] = "smartlocker/controller/1/log";
const char TOPIC_ACK[] = "smartlocker/controller/1/ack";
const char TOPIC_LOG_ACK[] = "smartlocker/controller/1/log/ack";
const char TOPIC_ALERT[] = "smartlocker/controller/1/alert";
const char TOPIC_TIME_REQUEST[] = "smartlocker/controller/1/time/request";
const char TOPIC_TIME_SYNC[] = "smartlocker/controller/1/time/sync";
const char TOPIC_STATE[] = "smartlocker/controller/1/state";

WiFiClient wifiClient;
MQTTClient mqttClient(512);

// -------------------- NVS / ARMARIOS --------------------
Preferences preferences;
struct LockerConfig {
    bool vinculado;
    uint16_t usuario;
    uint16_t senha;
};
LockerConfig armarios[NUM_ARMARIOS];
SemaphoreHandle_t mutexConfig = nullptr;

// -------------------- LOGS --------------------
struct __attribute__((packed)) LogEvento {
    uint16_t id;
    uint32_t timestamp;
    uint8_t armario;
    uint8_t tipo;
};
static_assert(sizeof(LogEvento) == 8, "LogEvento deve ter 8 bytes");

enum Evento : uint8_t {
    EVT_BOTAO = 0x01,
    EVT_AUTH_OK = 0x02,
    EVT_AUTH_ERRO = 0x03,
    EVT_TRAVA_LIBERADA = 0x04,
    EVT_TRAVA_REARMADA = 0x05,
    EVT_PORTA_ABERTA = 0x06,
    EVT_PORTA_FECHADA = 0x07,
    EVT_PRESENCA = 0x08,
    EVT_METAL = 0x09,
    EVT_LINK = 0x0A,
    EVT_UNLINK = 0x0B,
    EVT_OPEN_REMOTE = 0x0C,
    EVT_CHAMADA = 0x0D,
    EVT_PORTA_ABERTA_TEMPO = 0x0E,
    EVT_HORARIO_ATUALIZADO = 0x0F
};

const char *ARQUIVO_LOG = "/eventos.bin";
SemaphoreHandle_t mutexLog = nullptr;
uint16_t proximoEventoId = 1;
uint16_t ultimoLogConfirmado = 0;

// -------------------- PROTOCOLO --------------------
enum Comando : uint8_t {
    GET_LOGS = 0x01,
    LINK_LOCKER = 0x10,
    UNLINK_LOCKER = 0x11,
    OPEN_REMOTE = 0x12,
    GET_STATE = 0x13,
    TIME_REQUEST = 0x20,
    TIME_SYNC = 0x21
};

enum StatusAck : uint8_t {
    ACK_OK = 0x00,
    ACK_ARMARIO_INVALIDO = 0x01,
    ACK_PARAMETRO_INVALIDO = 0x02,
    ACK_COMANDO_INVALIDO = 0x03,
    ACK_OCUPADO = 0x04
};

// -------------------- FILAS FREERTOS --------------------
QueueHandle_t filaTravas = nullptr;
struct AlertaMQTT {
    uint8_t armario;
    uint32_t timestampAlerta;
    uint32_t timestampAbertura;
};
QueueHandle_t filaAlertas = nullptr;

struct MensagemLCD {
    char linha1[17];
    char linha2[17];
    uint16_t duracaoMs;
};
QueueHandle_t filaLCD = nullptr;

// -------------------- ESTADO TRAVA --------------------
uint8_t travaEmPulso = 0;
unsigned long inicioPulsoTrava = 0;

// -------------------- ESTADO PORTAS --------------------
struct EstadoPorta {
    bool aberta;
    bool leituraBruta;
    bool leituraEstavel;
    unsigned long instanteMudanca;
    unsigned long instanteAberturaMs;
    uint32_t timestampAbertura;
    bool alertaGerado;
};
EstadoPorta portas[NUM_ARMARIOS];
uint8_t ultimoByteSensores = 0;

// -------------------- LOGIN --------------------
enum EtapaLogin { LOGIN_USUARIO, LOGIN_SENHA };
EtapaLogin etapaLogin = LOGIN_USUARIO;
String usuarioDigitado = "";
String senhaDigitada = "";

// -------------------- LCD TEMPORARIO --------------------
unsigned long lcdTemporarioAte = 0;
bool lcdVoltarIdle = false;

// -------------------- MQTT RX --------------------
uint8_t comandoPendente[16];
int tamanhoComandoPendente = 0;
bool temComandoPendente = false;
bool temHorarioPendente = false;
uint32_t timestampHorarioPendente = 0;

// -------------------- ESTADO DA REDE --------------------
bool horarioSincronizado = false;
unsigned long ultimaTentativaWiFi = 0;
unsigned long ultimaTentativaMQTT = 0;
unsigned long ultimoPedidoHorario = 0;
unsigned long ultimoEnvioLogs = 0;
bool primeiraTentativaWiFi = true;
bool primeiraTentativaMQTT = true;
bool forcarEnvioLogs = false;

// ============================================================
// LCD
// ============================================================
void cancelarMensagemTemporariaLCD() {
    lcdTemporarioAte = 0;
    lcdVoltarIdle = false;
}

void mostrarIdle() {
    cancelarMensagemTemporariaLCD();
    lcd.clear();
    lcd.setCursor(0, 0); lcd.print("Smart Locker");
    lcd.setCursor(0, 1); lcd.print("Sistema livre");
}

void mostrarUsuario() {
    cancelarMensagemTemporariaLCD();
    lcd.clear();
    lcd.setCursor(0, 0); lcd.print("Usuario:");
    lcd.setCursor(0, 1); lcd.print(usuarioDigitado);
}

void mostrarSenha() {
    cancelarMensagemTemporariaLCD();
    lcd.clear();
    lcd.setCursor(0, 0); lcd.print("Senha:");
    lcd.setCursor(0, 1);
    for (unsigned int i = 0; i < senhaDigitada.length(); i++) lcd.print('*');
}

void mostrarAcessoNegado() {
    lcd.clear();
    lcd.setCursor(0, 0); lcd.print("Acesso negado");
    lcd.setCursor(0, 1); lcd.print("Tente novamente");
    lcdTemporarioAte = millis() + 700;
    lcdVoltarIdle = true;
}

void mostrarLiberacao(uint8_t armario) {
    cancelarMensagemTemporariaLCD();
    lcd.clear();
    lcd.setCursor(0, 0); lcd.print("Armario "); lcd.print(armario);
    lcd.setCursor(0, 1); lcd.print("Trava liberada");
}

void atualizarLCDTemporario() {
    if (lcdTemporarioAte == 0) return;
    if ((long)(millis() - lcdTemporarioAte) >= 0) {
        lcdTemporarioAte = 0;
        if (lcdVoltarIdle) {
            lcdVoltarIdle = false;
            mostrarIdle();
        }
    }
}

void enfileirarMensagemLCD(const char *linha1, const char *linha2, uint16_t duracaoMs = 1500) {
    if (!filaLCD) return;

    MensagemLCD msg{};
    snprintf(msg.linha1, sizeof(msg.linha1), "%-16.16s", linha1 ? linha1 : "");
    snprintf(msg.linha2, sizeof(msg.linha2), "%-16.16s", linha2 ? linha2 : "");
    msg.duracaoMs = duracaoMs;

    if (xQueueSend(filaLCD, &msg, 0) != pdTRUE) {
        MensagemLCD descartada{};
        xQueueReceive(filaLCD, &descartada, 0);
        xQueueSend(filaLCD, &msg, 0);
    }
}

void processarFilaLCD() {
    if (!filaLCD) return;

    MensagemLCD msg{};
    if (xQueueReceive(filaLCD, &msg, 0) != pdTRUE) return;

    lcd.clear();
    lcd.setCursor(0, 0);
    lcd.print(msg.linha1);
    lcd.setCursor(0, 1);
    lcd.print(msg.linha2);

    lcdTemporarioAte = millis() + msg.duracaoMs;
    lcdVoltarIdle = true;
}

// ============================================================
// TIMESTAMP
// ============================================================
uint32_t obterTimestamp() {
    time_t agora = time(nullptr);
    if (agora < 100000) return 0; // ainda nao sincronizou apos reboot
    return (uint32_t)agora;
}

// ============================================================
// LOG LOCAL - NUNCA PUBLICA MQTT
// ============================================================
uint16_t registrarEvento(uint8_t armario, uint8_t tipo) {
    if (xSemaphoreTake(mutexLog, portMAX_DELAY) != pdTRUE) return 0;

    LogEvento evento;
    evento.id = proximoEventoId++;
    if (proximoEventoId == 0) proximoEventoId = 1;
    evento.timestamp = obterTimestamp();
    evento.armario = armario;
    evento.tipo = tipo;

    bool gravou = false;
    File arquivo = LittleFS.open(ARQUIVO_LOG, FILE_APPEND);
    if (arquivo) {
        gravou = arquivo.write((uint8_t *)&evento, sizeof(evento)) == sizeof(evento);
        arquivo.close();
    }
    xSemaphoreGive(mutexLog);

    if (!gravou) {
        Serial.println("[LOG] ERRO ao gravar no LittleFS");
        return 0;
    }

    Serial.printf("[LOG LOCAL] ID=%u TS=%lu armario=%u evento=0x%02X\n",
                  evento.id, (unsigned long)evento.timestamp, evento.armario, evento.tipo);
    return evento.id;
}

void carregarEstadoLogs() {
    xSemaphoreTake(mutexLog, portMAX_DELAY);
    File arquivo = LittleFS.open(ARQUIVO_LOG, FILE_READ);
    bool encontrou = false;
    LogEvento ultimo{};
    if (arquivo) {
        while (arquivo.available() >= (int)sizeof(LogEvento)) {
            arquivo.read((uint8_t *)&ultimo, sizeof(ultimo));
            encontrou = true;
        }
        arquivo.close();
    }
    proximoEventoId = encontrou ? (uint16_t)(ultimo.id + 1) : 1;
    if (proximoEventoId == 0) proximoEventoId = 1;
    xSemaphoreGive(mutexLog);

    ultimoLogConfirmado = preferences.getUShort("logack", 0);
    if (!encontrou && ultimoLogConfirmado != 0) {
        ultimoLogConfirmado = 0;
        preferences.putUShort("logack", 0);
    }

    Serial.printf("[LOG] Proximo ID=%u | Ultimo ACK=%u\n", proximoEventoId, ultimoLogConfirmado);
}

size_t carregarLoteLogsPendentes(LogEvento *destino, size_t capacidade) {
    if (!destino || capacidade == 0) return 0;
    if (xSemaphoreTake(mutexLog, portMAX_DELAY) != pdTRUE) return 0;

    File arquivo = LittleFS.open(ARQUIVO_LOG, FILE_READ);
    if (!arquivo) {
        xSemaphoreGive(mutexLog);
        return 0;
    }

    size_t quantidade = 0;
    while (arquivo.available() >= (int)sizeof(LogEvento) && quantidade < capacidade) {
        LogEvento evento;
        arquivo.read((uint8_t *)&evento, sizeof(evento));
        if (evento.id > ultimoLogConfirmado) destino[quantidade++] = evento;
    }

    arquivo.close();
    xSemaphoreGive(mutexLog);
    return quantidade;
}

// Comandos remotos precisam ter prioridade mesmo durante sincronizacao de logs.
void processarComandoPendente();

// ============================================================
// MQTT PUBLICACAO - SOMENTE TASK DE REDE
// ============================================================
bool publicarBinario(const char *topico, const uint8_t *dados, size_t tamanho, int qos = 1) {
    if (!mqttClient.connected()) return false;
    return mqttClient.publish(topico, (const char *)dados, tamanho, false, qos);
}

bool publicarLog(const LogEvento &evento) {
    uint8_t p[8];
    p[0] = (evento.id >> 8) & 0xFF;
    p[1] = evento.id & 0xFF;
    p[2] = (evento.timestamp >> 24) & 0xFF;
    p[3] = (evento.timestamp >> 16) & 0xFF;
    p[4] = (evento.timestamp >> 8) & 0xFF;
    p[5] = evento.timestamp & 0xFF;
    p[6] = evento.armario;
    p[7] = evento.tipo;
    return publicarBinario(TOPIC_LOG, p, sizeof(p), 1);
}

bool enviarLogsPendentes() {
    if (!mqttClient.connected()) return false;

    Serial.println("[LOG MQTT] Enviando pendentes...");
    size_t total = 0;

    while (mqttClient.connected()) {
        LogEvento lote[TAMANHO_LOTE_LOG];
        size_t qtd = carregarLoteLogsPendentes(lote, TAMANHO_LOTE_LOG);
        if (qtd == 0) {
            Serial.printf("[LOG MQTT] Finalizado. Enviados=%u\n", (unsigned)total);
            return true;
        }

        for (size_t i = 0; i < qtd; i++) {
            // Nunca deixe o backlog de logs bloquear LINK/UNLINK/OPEN_REMOTE.
            mqttClient.loop();
            processarComandoPendente();

            const LogEvento &evento = lote[i];
            if (evento.id <= ultimoLogConfirmado) continue;

            if (!publicarLog(evento)) return false;

            unsigned long inicio = millis();
            while (mqttClient.connected() &&
                   ultimoLogConfirmado < evento.id &&
                   millis() - inicio < TIMEOUT_ACK_LOG_MS) {
                mqttClient.loop();
                processarComandoPendente();
                vTaskDelay(pdMS_TO_TICKS(5));
            }

            if (ultimoLogConfirmado < evento.id) {
                Serial.printf("[LOG MQTT] Timeout ACK ID=%u\n", evento.id);
                return false;
            }
            total++;
        }
    }
    return false;
}

// ============================================================
// TRAVAS
// ============================================================
bool armarioValido(uint8_t armario) {
    return armario >= 1 && armario <= NUM_ARMARIOS;
}

void desabilitarDecoder() {
    digitalWrite(DEC_ENABLE, LOW);
}

void selecionarArmario(uint8_t armario) {
    if (!armarioValido(armario)) return;
    uint8_t indice = armario - 1;
    desabilitarDecoder();
    digitalWrite(DEC_A, (indice & 0x01) ? HIGH : LOW);
    digitalWrite(DEC_B, (indice & 0x02) ? HIGH : LOW);
    digitalWrite(DEC_C, (indice & 0x04) ? HIGH : LOW);
    delayMicroseconds(10);
}

bool solicitarLiberacaoTrava(uint8_t armario) {
    if (!armarioValido(armario)) {
        Serial.printf("[FILA TRAVA] Armario invalido=%u\n", armario);
        return false;
    }

    if (!filaTravas) {
        Serial.println("[FILA TRAVA] Handle nulo");
        return false;
    }

    BaseType_t resultado = xQueueSend(filaTravas, &armario, 0);
    Serial.printf("[FILA TRAVA] xQueueSend armario=%u -> %s\n",
                  armario, resultado == pdTRUE ? "OK" : "FALHOU");
    return resultado == pdTRUE;
}

void iniciarProximoPulso() {
    if (travaEmPulso != 0) return;

    uint8_t armario = 0;
    if (xQueueReceive(filaTravas, &armario, 0) != pdTRUE) return;

    Serial.printf("[TRAVA] Retirado da fila armario=%u\n", armario);

    selecionarArmario(armario);
    Serial.printf("[TRAVA] Decoder selecionado A=%d B=%d C=%d\n",
                  digitalRead(DEC_A), digitalRead(DEC_B), digitalRead(DEC_C));

    digitalWrite(DEC_ENABLE, HIGH);
    Serial.printf("[TRAVA] DEC_ENABLE=%d\n", digitalRead(DEC_ENABLE));

    travaEmPulso = armario;
    inicioPulsoTrava = millis();

    // A acao fisica tem prioridade. Registro e LCD vem depois.
    Serial.printf("[TRAVA] Liberada armario %u\n", armario);

    uint16_t idEvento = registrarEvento(armario, EVT_TRAVA_LIBERADA);
    if (idEvento == 0) {
        Serial.println("[TRAVA] AVISO: falha ao salvar EVT_TRAVA_LIBERADA");
    }

    mostrarLiberacao(armario);
}

void finalizarPulsoTrava() {
    if (travaEmPulso == 0) return;
    uint8_t armario = travaEmPulso;
    desabilitarDecoder();
    travaEmPulso = 0;
    registrarEvento(armario, EVT_TRAVA_REARMADA);
    Serial.printf("[TRAVA] Rearmada armario %u | DEC_ENABLE=%d\n", armario, digitalRead(DEC_ENABLE));
    mostrarIdle();
}

void processarFilaTravas() {
    if (travaEmPulso != 0 && millis() - inicioPulsoTrava >= TEMPO_PULSO_TRAVA_MS)
        finalizarPulsoTrava();
    if (travaEmPulso == 0) iniciarProximoPulso();
}

// ============================================================
// 74LS165 / SENSORES
// ============================================================
uint8_t ler74LS165() {
    uint8_t valor = 0;
    digitalWrite(SR_LOAD, LOW);
    delayMicroseconds(10);
    digitalWrite(SR_LOAD, HIGH);
    delayMicroseconds(10);

    for (uint8_t i = 0; i < 8; i++) {
        valor <<= 1;
        if (digitalRead(SR_DATA)) valor |= 0x01;
        digitalWrite(SR_CLOCK, HIGH);
        delayMicroseconds(10);
        digitalWrite(SR_CLOCK, LOW);
        delayMicroseconds(10);
    }
    return valor;
}

bool bitPortaFechada(uint8_t byteSensores, uint8_t armario) {
    return (byteSensores >> (armario - 1)) & 0x01;
}

bool lerPortaAberta(uint8_t byteSensores, uint8_t armario) {
    return !bitPortaFechada(byteSensores, armario); // 0 aberta, 1 fechada
}

void imprimirSensores(uint8_t valor) {
    Serial.print("[SENSORES] H..A=");
    for (int8_t i = 7; i >= 0; i--) Serial.print((valor >> i) & 0x01);
    Serial.printf(" HEX=0x%02X\n", valor);
    for (uint8_t a = 1; a <= NUM_ARMARIOS; a++) {
        Serial.printf("  Armario %u: %s\n", a,
                      bitPortaFechada(valor, a) ? "FECHADA / METAL" : "ABERTA / SEM METAL");
    }
}

void inicializarSensores() {
    uint8_t valor = ler74LS165();
    ultimoByteSensores = valor;
    unsigned long agora = millis();

    for (uint8_t a = 1; a <= NUM_ARMARIOS; a++) {
        uint8_t i = a - 1;
        bool aberta = lerPortaAberta(valor, a);
        portas[i].aberta = aberta;
        portas[i].leituraBruta = aberta;
        portas[i].leituraEstavel = aberta;
        portas[i].instanteMudanca = agora;
        portas[i].instanteAberturaMs = aberta ? agora : 0;
        portas[i].timestampAbertura = aberta ? obterTimestamp() : 0;
        portas[i].alertaGerado = false;
    }
    imprimirSensores(valor);
}

// ============================================================
// ALERTAS
// ============================================================
void enfileirarAlertaPorta(uint8_t armario, uint32_t timestampAbertura) {
    if (!filaAlertas) return;
    AlertaMQTT alerta{armario, obterTimestamp(), timestampAbertura};
    if (xQueueSend(filaAlertas, &alerta, 0) != pdTRUE) {
        Serial.println("[ALERTA] Fila cheia; evento 0x0E continua salvo no LittleFS");
    }
}

bool publicarAlerta(const AlertaMQTT &a) {
    uint8_t p[10];
    p[0] = 0x01;
    p[1] = a.armario;
    p[2] = (a.timestampAlerta >> 24) & 0xFF;
    p[3] = (a.timestampAlerta >> 16) & 0xFF;
    p[4] = (a.timestampAlerta >> 8) & 0xFF;
    p[5] = a.timestampAlerta & 0xFF;
    p[6] = (a.timestampAbertura >> 24) & 0xFF;
    p[7] = (a.timestampAbertura >> 16) & 0xFF;
    p[8] = (a.timestampAbertura >> 8) & 0xFF;
    p[9] = a.timestampAbertura & 0xFF;
    return publicarBinario(TOPIC_ALERT, p, sizeof(p), 1);
}

void processarFilaAlertasMQTT() {
    if (!mqttClient.connected()) return;
    AlertaMQTT a;
    while (xQueueReceive(filaAlertas, &a, 0) == pdTRUE) {
        if (!publicarAlerta(a)) {
            xQueueSendToFront(filaAlertas, &a, 0);
            return;
        }
        mqttClient.loop();
    }
}

void tratarMudancaPorta(uint8_t armario, bool aberta) {
    uint8_t i = armario - 1;
    portas[i].aberta = aberta;

    if (aberta) {
        portas[i].instanteAberturaMs = millis();
        portas[i].timestampAbertura = obterTimestamp();
        portas[i].alertaGerado = false;
        registrarEvento(armario, EVT_PORTA_ABERTA);
        Serial.printf("[PORTA] Armario %u ABERTA\n", armario);
    } else {
        portas[i].instanteAberturaMs = 0;
        portas[i].timestampAbertura = 0;
        portas[i].alertaGerado = false;
        registrarEvento(armario, EVT_PORTA_FECHADA);
        Serial.printf("[PORTA] Armario %u FECHADA\n", armario);
    }
}

void monitorarSensores() {
    static unsigned long ultimaLeitura = 0;
    if (millis() - ultimaLeitura < 50) return;
    ultimaLeitura = millis();

    uint8_t valor = ler74LS165();
    ultimoByteSensores = valor;

    for (uint8_t a = 1; a <= NUM_ARMARIOS; a++) {
        uint8_t i = a - 1;
        bool leitura = lerPortaAberta(valor, a);

        if (leitura != portas[i].leituraBruta) {
            portas[i].leituraBruta = leitura;
            portas[i].instanteMudanca = millis();
        }

        if (portas[i].leituraBruta != portas[i].leituraEstavel &&
            millis() - portas[i].instanteMudanca >= DEBOUNCE_SENSOR_MS) {
            portas[i].leituraEstavel = portas[i].leituraBruta;
            tratarMudancaPorta(a, portas[i].leituraEstavel);
        }

        if (portas[i].aberta && !portas[i].alertaGerado &&
            millis() - portas[i].instanteAberturaMs >= TEMPO_ALERTA_PORTA_MS) {
            portas[i].alertaGerado = true;
            registrarEvento(a, EVT_PORTA_ABERTA_TEMPO);
            enfileirarAlertaPorta(a, portas[i].timestampAbertura);
            Serial.printf("[ALERTA] Armario %u aberto por muito tempo\n", a);
        }
    }
}

// ============================================================
// NVS / ARMARIOS
// ============================================================
void montarChave(char *destino, size_t tamanho, uint8_t armario, char campo) {
    snprintf(destino, tamanho, "l%u%c", armario, campo);
}

void salvarArmario(uint8_t armario) {
    uint8_t i = armario - 1;
    char chave[8];
    montarChave(chave, sizeof(chave), armario, 'v'); preferences.putBool(chave, armarios[i].vinculado);
    montarChave(chave, sizeof(chave), armario, 'u'); preferences.putUShort(chave, armarios[i].usuario);
    montarChave(chave, sizeof(chave), armario, 'p'); preferences.putUShort(chave, armarios[i].senha);
}

void carregarArmarios() {
    for (uint8_t a = 1; a <= NUM_ARMARIOS; a++) {
        uint8_t i = a - 1;
        char chave[8];
        montarChave(chave, sizeof(chave), a, 'v'); armarios[i].vinculado = preferences.getBool(chave, false);
        montarChave(chave, sizeof(chave), a, 'u'); armarios[i].usuario = preferences.getUShort(chave, 0);
        montarChave(chave, sizeof(chave), a, 'p'); armarios[i].senha = preferences.getUShort(chave, 0);
        Serial.printf("[NVS] Armario %u -> %s\n", a, armarios[i].vinculado ? "vinculado" : "livre");
    }
}

// ============================================================
// LOGIN / TECLADO
// ============================================================
int encontrarArmarioPorUsuario(uint16_t usuario, uint16_t senha, bool &senhaCorreta) {
    senhaCorreta = false;
    if (xSemaphoreTake(mutexConfig, pdMS_TO_TICKS(50)) != pdTRUE) return 0;

    int encontrado = 0;
    for (uint8_t i = 0; i < NUM_ARMARIOS; i++) {
        if (armarios[i].vinculado && armarios[i].usuario == usuario) {
            encontrado = i + 1;
            senhaCorreta = armarios[i].senha == senha;
            break;
        }
    }
    xSemaphoreGive(mutexConfig);
    return encontrado;
}

void autenticar() {
    uint16_t usuario = usuarioDigitado.toInt();
    uint16_t senha = senhaDigitada.toInt();
    bool senhaCorreta = false;
    int armario = encontrarArmarioPorUsuario(usuario, senha, senhaCorreta);

    if (armario > 0 && senhaCorreta) {
        registrarEvento(armario, EVT_AUTH_OK);
        Serial.printf("[AUTH] OK -> armario %d\n", armario);
        if (!solicitarLiberacaoTrava(armario)) Serial.println("[AUTH] Fila de travas cheia");
        usuarioDigitado = "";
        senhaDigitada = "";
        etapaLogin = LOGIN_USUARIO;
    } else {
        registrarEvento(armario > 0 ? armario : 0, EVT_AUTH_ERRO);
        Serial.println("[AUTH] NEGADA");
        usuarioDigitado = "";
        senhaDigitada = "";
        etapaLogin = LOGIN_USUARIO;
        mostrarAcessoNegado();
    }
}

void processarTeclado() {
    char tecla = teclado.getKey();
    if (!tecla) return;
    Serial.printf("[TECLADO] %c\n", tecla);

    if (tecla == '*') {
        if (etapaLogin == LOGIN_USUARIO) {
            if (usuarioDigitado.length() > 0) usuarioDigitado.remove(usuarioDigitado.length() - 1);
            mostrarUsuario();
        } else {
            if (senhaDigitada.length() > 0) senhaDigitada.remove(senhaDigitada.length() - 1);
            mostrarSenha();
        }
        return;
    }

    if (tecla == '#') {
        if (usuarioDigitado.length() == 4 && senhaDigitada.length() == 4) autenticar();
        else Serial.println("[AUTH] Usuario/senha incompletos");
        return;
    }

    if (tecla < '0' || tecla > '9') return;

    if (etapaLogin == LOGIN_USUARIO) {
        if (usuarioDigitado.length() < 4) {
            usuarioDigitado += tecla;
            mostrarUsuario();
        }
        if (usuarioDigitado.length() == 4) {
            etapaLogin = LOGIN_SENHA;
            mostrarSenha();
        }
    } else {
        if (senhaDigitada.length() < 4) {
            senhaDigitada += tecla;
            mostrarSenha();
        }
    }
}

// ============================================================
// BOTAO
// ============================================================
void processarBotao() {
    bool leitura = digitalRead(BOTAO_PIN);
    if (leitura != ultimoEstadoBotao) {
        instanteMudancaBotao = millis();
        ultimoEstadoBotao = leitura;
    }
    if (millis() - instanteMudancaBotao < DEBOUNCE_BOTAO_MS) return;

    if (leitura != estadoEstavelBotao) {
        estadoEstavelBotao = leitura;
        if (estadoEstavelBotao == LOW) {
            registrarEvento(travaEmPulso != 0 ? travaEmPulso : 0, EVT_BOTAO);
            if (travaEmPulso != 0) finalizarPulsoTrava();
            else Serial.println("[BOTAO] Pressionado");
        }
    }
}

// ============================================================
// ACK / COMANDOS MQTT
// ============================================================
void enviarACK(uint8_t comando, uint8_t armario, uint8_t status) {
    uint8_t resposta[3] = {comando, armario, status};
    bool ok = publicarBinario(TOPIC_ACK, resposta, sizeof(resposta), 1);
    Serial.printf("[ACK TX] cmd=0x%02X armario=%u status=0x%02X -> %s\n",
                  comando, armario, status, ok ? "OK" : "FALHOU");
}

bool publicarEstadoArmarios() {
    if (!mqttClient.connected()) return false;

    uint8_t pacote[18] = {0};
    pacote[0] = GET_STATE;
    uint8_t mascara = 0;

    if (xSemaphoreTake(mutexConfig, pdMS_TO_TICKS(500)) != pdTRUE) {
        Serial.println("[STATE] mutexConfig ocupado");
        return false;
    }

    for (uint8_t a = 1; a <= NUM_ARMARIOS; a++) {
        uint8_t i = a - 1;
        if (armarios[i].vinculado) mascara |= (1U << i);

        uint16_t usuario = armarios[i].vinculado ? armarios[i].usuario : 0;
        pacote[2 + i * 2] = (usuario >> 8) & 0xFF;
        pacote[3 + i * 2] = usuario & 0xFF;
    }

    pacote[1] = mascara;
    xSemaphoreGive(mutexConfig);

    bool ok = mqttClient.publish(
        TOPIC_STATE,
        (const char *)pacote,
        sizeof(pacote),
        true,
        1
    );

    Serial.printf("[STATE TX] mask=0x%02X -> %s\n", mascara, ok ? "OK" : "FALHOU");
    return ok;
}

void executarGetState(uint8_t *d, int tamanho) {
    if (tamanho != 1 || d[0] != GET_STATE) {
        enviarACK(GET_STATE, 0, ACK_PARAMETRO_INVALIDO);
        return;
    }

    bool ok = publicarEstadoArmarios();
    enviarACK(GET_STATE, 0, ok ? ACK_OK : ACK_OCUPADO);
}

void executarLink(uint8_t *d, int tamanho) {
    if (tamanho != 6) {
        enfileirarMensagemLCD("Falha vinculo", "Pacote invalido");
        enviarACK(LINK_LOCKER, 0, ACK_PARAMETRO_INVALIDO);
        return;
    }
    uint8_t armario = d[1];
    if (!armarioValido(armario)) {
        enfileirarMensagemLCD("Falha vinculo", "Armario invalido");
        enviarACK(LINK_LOCKER, armario, ACK_ARMARIO_INVALIDO);
        return;
    }

    uint16_t usuario = ((uint16_t)d[2] << 8) | d[3];
    uint16_t senha = ((uint16_t)d[4] << 8) | d[5];
    if (usuario > 9999 || senha > 9999) {
        enfileirarMensagemLCD("Falha vinculo", "Dados invalidos");
        enviarACK(LINK_LOCKER, armario, ACK_PARAMETRO_INVALIDO);
        return;
    }

    if (xSemaphoreTake(mutexConfig, pdMS_TO_TICKS(500)) != pdTRUE) {
        enfileirarMensagemLCD("Falha vinculo", "Sistema ocupado");
        enviarACK(LINK_LOCKER, armario, ACK_OCUPADO);
        return;
    }

    bool duplicado = false;
    for (uint8_t i = 0; i < NUM_ARMARIOS; i++) {
        uint8_t outro = i + 1;
        if (outro != armario && armarios[i].vinculado && armarios[i].usuario == usuario) {
            duplicado = true; break;
        }
    }
    if (duplicado) {
        xSemaphoreGive(mutexConfig);
        enfileirarMensagemLCD("Falha vinculo", "Usuario em uso");
        enviarACK(LINK_LOCKER, armario, ACK_PARAMETRO_INVALIDO);
        return;
    }

    uint8_t i = armario - 1;
    armarios[i].vinculado = true;
    armarios[i].usuario = usuario;
    armarios[i].senha = senha;
    salvarArmario(armario);
    xSemaphoreGive(mutexConfig);

    registrarEvento(armario, EVT_LINK);

    char linha1[17];
    snprintf(linha1, sizeof(linha1), "Armario %u", armario);
    enfileirarMensagemLCD(linha1, "Vinculado");

    enviarACK(LINK_LOCKER, armario, ACK_OK);
    publicarEstadoArmarios();
    Serial.printf("[LINK] Armario %u vinculado\n", armario);
}

void executarUnlink(uint8_t *d, int tamanho) {
    if (tamanho != 2) {
        enfileirarMensagemLCD("Falha desvinc.", "Pacote invalido");
        enviarACK(UNLINK_LOCKER, 0, ACK_PARAMETRO_INVALIDO);
        return;
    }
    uint8_t armario = d[1];
    if (!armarioValido(armario)) {
        enfileirarMensagemLCD("Falha desvinc.", "Armario invalido");
        enviarACK(UNLINK_LOCKER, armario, ACK_ARMARIO_INVALIDO);
        return;
    }

    if (xSemaphoreTake(mutexConfig, pdMS_TO_TICKS(500)) != pdTRUE) {
        enfileirarMensagemLCD("Falha desvinc.", "Sistema ocupado");
        enviarACK(UNLINK_LOCKER, armario, ACK_OCUPADO);
        return;
    }

    uint8_t i = armario - 1;
    armarios[i].vinculado = false;
    armarios[i].usuario = 0;
    armarios[i].senha = 0;
    salvarArmario(armario);
    xSemaphoreGive(mutexConfig);

    registrarEvento(armario, EVT_UNLINK);

    char linha1[17];
    snprintf(linha1, sizeof(linha1), "Armario %u", armario);
    enfileirarMensagemLCD(linha1, "Desvinculado");

    enviarACK(UNLINK_LOCKER, armario, ACK_OK);
    publicarEstadoArmarios();
    Serial.printf("[UNLINK] Armario %u desvinculado\n", armario);
}

void executarAberturaRemota(uint8_t *d, int tamanho) {
    Serial.printf("[OPEN_REMOTE] Inicio handler len=%d\n", tamanho);

    if (tamanho != 2) {
        Serial.println("[OPEN_REMOTE] ERRO pacote invalido");
        enfileirarMensagemLCD("Falha abertura", "Pacote invalido");
        enviarACK(OPEN_REMOTE, 0, ACK_PARAMETRO_INVALIDO);
        return;
    }

    uint8_t armario = d[1];
    Serial.printf("[OPEN_REMOTE] Armario solicitado=%u\n", armario);

    if (!armarioValido(armario)) {
        Serial.println("[OPEN_REMOTE] ERRO armario invalido");
        enfileirarMensagemLCD("Falha abertura", "Armario invalido");
        enviarACK(OPEN_REMOTE, armario, ACK_ARMARIO_INVALIDO);
        return;
    }

    UBaseType_t antes = uxQueueMessagesWaiting(filaTravas);
    Serial.printf("[OPEN_REMOTE] Fila antes=%u/%u\n",
                  (unsigned)antes, (unsigned)NUM_ARMARIOS);

    if (!solicitarLiberacaoTrava(armario)) {
        Serial.println("[OPEN_REMOTE] ERRO fila cheia");
        enfileirarMensagemLCD("Falha abertura", "Fila ocupada");
        enviarACK(OPEN_REMOTE, armario, ACK_OCUPADO);
        return;
    }

    UBaseType_t depois = uxQueueMessagesWaiting(filaTravas);
    Serial.printf("[OPEN_REMOTE] Fila aceita. Depois=%u/%u\n",
                  (unsigned)depois, (unsigned)NUM_ARMARIOS);

    // ACK de aplicacao deve confirmar a aceitacao do comando imediatamente.
    // Nao dependa de LittleFS, LCD ou do processamento fisico da trava.
    enviarACK(OPEN_REMOTE, armario, ACK_OK);
    Serial.println("[OPEN_REMOTE] ACK enviado; agora registrando evento");

    // O log continua sendo persistido, mas nao pode atrasar o ACK do comando.
    uint16_t idEvento = registrarEvento(armario, EVT_OPEN_REMOTE);
    if (idEvento == 0) {
        Serial.println("[OPEN_REMOTE] AVISO: falha ao persistir EVT_OPEN_REMOTE");
    } else {
        Serial.printf("[OPEN_REMOTE] Evento persistido ID=%u\n", idEvento);
    }

    Serial.printf("[OPEN_REMOTE] Armario %u enfileirado com sucesso\n", armario);
}

// ============================================================
// HORARIO
// ============================================================
void solicitarHorarioServidor() {
    if (!mqttClient.connected()) return;
    uint8_t pacote[1] = {TIME_REQUEST};
    publicarBinario(TOPIC_TIME_REQUEST, pacote, sizeof(pacote), 1);
    ultimoPedidoHorario = millis();
    Serial.println("[TIME] TIME_REQUEST enviado");
}

void aplicarHorarioPendente() {
    if (!temHorarioPendente) return;
    temHorarioPendente = false;

    struct timeval tv;
    tv.tv_sec = timestampHorarioPendente;
    tv.tv_usec = 0;

    if (settimeofday(&tv, nullptr) == 0) {
        horarioSincronizado = true;
        Serial.printf("[TIME] Atualizado: %lu\n", (unsigned long)timestampHorarioPendente);
        registrarEvento(0, EVT_HORARIO_ATUALIZADO);
    } else {
        Serial.println("[TIME] Falha em settimeofday()");
    }
}

void verificarSincronizacaoHorario() {
    if (!mqttClient.connected()) return;
    unsigned long intervalo = horarioSincronizado ? INTERVALO_SYNC_HORA_MS : INTERVALO_RETRY_HORA_MS;
    if (millis() - ultimoPedidoHorario >= intervalo) solicitarHorarioServidor();
}

// ============================================================
// CALLBACK MQTT
// ============================================================
void mensagemRecebida(MQTTClient *client, char topic[], char bytes[], int length) {
    Serial.printf("[MQTT RX] topic=%s len=%d\n", topic, length);

    if (strcmp(topic, TOPIC_CMD) == 0) {
        if (length <= 0 || length > (int)sizeof(comandoPendente)) {
            Serial.printf("[CMD RX] Pacote invalido len=%d\n", length);
            return;
        }

        Serial.printf("[CMD RX] len=%d HEX=", length);
        for (int i = 0; i < length; i++) {
            Serial.printf("%02X", (uint8_t)bytes[i]);
            if (i + 1 < length) Serial.print(" ");
        }
        Serial.println();

        memcpy(comandoPendente, bytes, length);
        tamanhoComandoPendente = length;
        temComandoPendente = true;
        return;
    }

    if (strcmp(topic, TOPIC_TIME_SYNC) == 0) {
        if (length != 5 || (uint8_t)bytes[0] != TIME_SYNC) return;
        timestampHorarioPendente =
            ((uint32_t)(uint8_t)bytes[1] << 24) |
            ((uint32_t)(uint8_t)bytes[2] << 16) |
            ((uint32_t)(uint8_t)bytes[3] << 8) |
            (uint32_t)(uint8_t)bytes[4];
        temHorarioPendente = true;
        return;
    }

    if (strcmp(topic, TOPIC_LOG_ACK) == 0) {
        if (length != 2) {
            Serial.printf("[LOG ACK RX] tamanho invalido=%d\n", length);
            return;
        }

        uint16_t id = ((uint16_t)(uint8_t)bytes[0] << 8) | (uint16_t)(uint8_t)bytes[1];
        Serial.printf("[LOG ACK RX] ID=%u | ultimo=%u\n", id, ultimoLogConfirmado);

        if (id > ultimoLogConfirmado) {
            ultimoLogConfirmado = id;
            preferences.putUShort("logack", ultimoLogConfirmado);
            Serial.printf("[LOG ACK] AVANCOU PARA ID=%u\n", ultimoLogConfirmado);
        } else {
            Serial.println("[LOG ACK] Recebido, mas nao precisava avancar");
        }
        return;
    }
}

void processarComandoPendente() {
    if (!temComandoPendente) return;
    temComandoPendente = false;
    uint8_t comando = comandoPendente[0];
    Serial.printf("[CMD] Processando 0x%02X len=%d\n", comando, tamanhoComandoPendente);

    switch (comando) {
        case GET_LOGS:      forcarEnvioLogs = true; break;
        case LINK_LOCKER:   executarLink(comandoPendente, tamanhoComandoPendente); break;
        case UNLINK_LOCKER: executarUnlink(comandoPendente, tamanhoComandoPendente); break;
        case OPEN_REMOTE:   executarAberturaRemota(comandoPendente, tamanhoComandoPendente); break;
        case GET_STATE:     executarGetState(comandoPendente, tamanhoComandoPendente); break;
        default:            enviarACK(comando, 0, ACK_COMANDO_INVALIDO); break;
    }
}

// ============================================================
// REDE / RECONEXAO
// ============================================================
bool passouIntervalo(unsigned long agora, unsigned long anterior, unsigned long intervalo) {
    return agora - anterior >= intervalo;
}

void tentarConectarWiFi() {
    if (WiFi.status() == WL_CONNECTED) {
        primeiraTentativaWiFi = false;
        return;
    }

    unsigned long agora = millis();
    if (!primeiraTentativaWiFi &&
        !passouIntervalo(agora, ultimaTentativaWiFi, INTERVALO_RECONEXAO_MS)) return;

    primeiraTentativaWiFi = false;
    ultimaTentativaWiFi = agora;
    Serial.println("[WIFI] Tentando conectar...");
    WiFi.disconnect(false);
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
}

bool conectarMQTT() {
    if (WiFi.status() != WL_CONNECTED) return false;
    if (mqttClient.connected()) return true;

    unsigned long agora = millis();
    if (!primeiraTentativaMQTT &&
        !passouIntervalo(agora, ultimaTentativaMQTT, INTERVALO_RECONEXAO_MS)) return false;

    primeiraTentativaMQTT = false;
    ultimaTentativaMQTT = agora;
    Serial.println("[MQTT] Tentando conectar...");

    // Se bloquear alguns segundos, somente esta task fica parada.
    // O teclado continua na task de hardware.
    if (!mqttClient.connect("esp32-smartlocker-1")) {
        Serial.println("[MQTT] Falhou. Nova tentativa em 1 minuto.");
        return false;
    }

    bool subCmd = mqttClient.subscribe(TOPIC_CMD, 1);
    bool subTime = mqttClient.subscribe(TOPIC_TIME_SYNC, 1);
    bool subLogAck = mqttClient.subscribe(TOPIC_LOG_ACK, 1);

    Serial.println("[MQTT] Conectado");
    Serial.printf("[MQTT SUB] CMD=%s | TIME_SYNC=%s | LOG_ACK=%s\n",
                  subCmd ? "OK" : "FALHOU",
                  subTime ? "OK" : "FALHOU",
                  subLogAck ? "OK" : "FALHOU");

    if (!subCmd || !subTime || !subLogAck) {
        Serial.println("[MQTT] ERRO: assinatura falhou; desconectando.");
        mqttClient.disconnect();
        return false;
    }

    publicarEstadoArmarios();
    solicitarHorarioServidor();

    if (ENVIO_LOG_AUTOMATICO) {
        forcarEnvioLogs = true;
    } else {
        Serial.println("[LOG MQTT] Automatico DESATIVADO para diagnostico/demo");
    }

    return true;
}

// ============================================================
// SERIAL DE TESTE
// ============================================================
void mostrarMenuSerial() {
    Serial.println("\n======= TESTE LOCAL =======");
    Serial.println("h   -> ajuda");
    Serial.println("s   -> sensores");
    Serial.println("1-8 -> testar trava");
    Serial.println("x   -> desabilitar decoder");
    Serial.println("===========================");
}

void processarSerialTeste() {
    if (!Serial.available()) return;
    char c = Serial.read();
    if (c == '\n' || c == '\r') return;

    if (c >= '1' && c <= '8') {
        uint8_t armario = c - '0';
        solicitarLiberacaoTrava(armario);
        return;
    }

    switch (c) {
        case 'h': case 'H': mostrarMenuSerial(); break;
        case 's': case 'S': imprimirSensores(ler74LS165()); break;
        case 'x': case 'X':
            desabilitarDecoder();
            travaEmPulso = 0;
            xQueueReset(filaTravas);
            mostrarIdle();
            Serial.println("[TESTE] Decoder desabilitado");
            break;
    }
}

// ============================================================
// TASK DE HARDWARE
// ============================================================
void tarefaHardware(void *parametro) {
    Serial.println("[TASK HW] Iniciada");
    for (;;) {
        processarTeclado();
        processarBotao();
        processarFilaTravas();
        monitorarSensores();
        atualizarLCDTemporario();
        processarFilaLCD();
        processarSerialTeste();
        vTaskDelay(pdMS_TO_TICKS(5));
    }
}

// ============================================================
// TASK DE REDE
// ============================================================
void tarefaRede(void *parametro) {
    Serial.println("[TASK REDE] Iniciada");
    bool mqttEstavaConectado = false;
    unsigned long ultimoStatusRede = 0;

    for (;;) {
        tentarConectarWiFi();

        if (WiFi.status() == WL_CONNECTED && !mqttClient.connected()) {
            conectarMQTT();
        }

        if (mqttClient.connected()) {
            mqttClient.loop();
            processarComandoPendente();
            aplicarHorarioPendente();
            verificarSincronizacaoHorario();
            processarFilaAlertasMQTT();

            bool ciclo15min =
                ENVIO_LOG_AUTOMATICO &&
                passouIntervalo(millis(), ultimoEnvioLogs, INTERVALO_ENVIO_LOG_MS);

            // GET_LOGS manual define forcarEnvioLogs e continua funcionando.
            if (forcarEnvioLogs || ciclo15min) {
                forcarEnvioLogs = false;
                bool terminou = enviarLogsPendentes();
                ultimoEnvioLogs = millis();
                if (!terminou) {
                    Serial.println("[LOG MQTT] Ciclo interrompido; logs continuam no LittleFS");
                }
            }
        }

        if (mqttEstavaConectado && !mqttClient.connected()) {
            Serial.println("[MQTT] Desconectado; nova tentativa em ate 1 minuto");
        }
        mqttEstavaConectado = mqttClient.connected();

        if (millis() - ultimoStatusRede >= 10000) {
            ultimoStatusRede = millis();
            Serial.printf("[NET STATUS] WiFi=%s IP=%s MQTT=%s\n",
                          WiFi.status() == WL_CONNECTED ? "OK" : "OFF",
                          WiFi.localIP().toString().c_str(),
                          mqttClient.connected() ? "OK" : "OFF");
        }

        vTaskDelay(pdMS_TO_TICKS(10));
    }
}

// ============================================================
// SETUP
// ============================================================
void setup() {
    Serial.begin(115200);
    delay(500);

    Serial.println("\n====================================");
    Serial.println(" SMART LOCKER V5.4 - MQTT RX DIAGNOSTICO");
    Serial.println("====================================");

    mutexLog = xSemaphoreCreateMutex();
    mutexConfig = xSemaphoreCreateMutex();
    filaTravas = xQueueCreate(NUM_ARMARIOS, sizeof(uint8_t));
    filaAlertas = xQueueCreate(NUM_ARMARIOS, sizeof(AlertaMQTT));
    filaLCD = xQueueCreate(4, sizeof(MensagemLCD));

    if (!mutexLog || !mutexConfig || !filaTravas || !filaAlertas || !filaLCD) {
        Serial.println("[FATAL] Falha ao criar objetos FreeRTOS");
        while (true) delay(1000);
    }

    // 74LS138
    pinMode(DEC_A, OUTPUT);
    pinMode(DEC_B, OUTPUT);
    pinMode(DEC_C, OUTPUT);
    pinMode(DEC_ENABLE, OUTPUT);
    digitalWrite(DEC_A, LOW);
    digitalWrite(DEC_B, LOW);
    digitalWrite(DEC_C, LOW);
    desabilitarDecoder();

    // 74LS165
    pinMode(SR_LOAD, OUTPUT);
    pinMode(SR_CLOCK, OUTPUT);
    pinMode(SR_DATA, INPUT);
    digitalWrite(SR_LOAD, HIGH);
    digitalWrite(SR_CLOCK, LOW);

    // Botao
    pinMode(BOTAO_PIN, INPUT);
    ultimoEstadoBotao = digitalRead(BOTAO_PIN);
    estadoEstavelBotao = ultimoEstadoBotao;

    // LCD
    Wire.begin(SDA_PIN, SCL_PIN);
    lcd.init();
    lcd.backlight();
    mostrarIdle(); // aparece antes de Wi-Fi/MQTT

    // NVS
    preferences.begin("lockers", false);
    carregarArmarios();

    // LittleFS
    if (!LittleFS.begin(true)) {
        Serial.println("[FATAL] Falha ao montar LittleFS");
        while (true) delay(1000);
    }
    Serial.println("[FS] LittleFS OK");
    carregarEstadoLogs();

    // Sensores
    inicializarSensores();

    // MQTT
    mqttClient.begin(MQTT_SERVER, MQTT_PORT, wifiClient);
    mqttClient.onMessageAdvanced(mensagemRecebida);

    WiFi.mode(WIFI_STA);
    WiFi.setAutoReconnect(false);

    primeiraTentativaWiFi = true;
    primeiraTentativaMQTT = true;
    ultimoEnvioLogs = millis();

    mostrarMenuSerial();

    BaseType_t hw = xTaskCreatePinnedToCore(
        tarefaHardware, "HardwareTask", 8192, nullptr, 2, nullptr, 1);

    BaseType_t rede = xTaskCreatePinnedToCore(
        tarefaRede, "NetworkTask", 12288, nullptr, 1, nullptr, 0);

    if (hw != pdPASS || rede != pdPASS) {
        Serial.println("[FATAL] Falha ao criar tasks");
        while (true) delay(1000);
    }

    Serial.println("[SISTEMA] Inicializacao concluida");
}

void loop() {
    delay(1000);
}

