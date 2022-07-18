#include <M5Stack.h>
#include <HTTPClient.h>
#include <WiFi.h>
#include <ArduinoJson.h>

#define WIFI_SSID "wifi-2.4"
#define WIFI_PASSWORD "00000000"

// Json設定
StaticJsonDocument<255> json_request;
char buffer[255];

// カウント初期化
int count = 0;

void setup() {
  M5.begin();

  // Wi-Fi接続
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  Serial.print("connecting");
  while (WiFi.status() != WL_CONNECTED) {
    Serial.print(".");
    delay(500);
  }
  Serial.println();

  // WiFi Connected
  Serial.println("\nWiFi Connected.");
  Serial.println(WiFi.localIP());
  M5.Lcd.setTextSize(3);
  M5.Lcd.setCursor(10, 100);
  M5.Lcd.println("Button Click!");


}

// api
const char *host = "https://api-v1-4z3wtnbd5a-an.a.run.app/rswq";

// カウント値送信
void sendCount() {
  json_request["created_at"] = "2022-07-19 22:22:00";
  json_request["location"] = "aquarium_001";
  json_request["sensor"] = "thermometer";
  json_request["measurements"] = count;
  serializeJson(json_request, buffer, sizeof(buffer));

  HTTPClient http;
  http.begin(host);
  http.addHeader("Content-Type", "application/json");
  int status_code = http.POST((uint8_t*)buffer, strlen(buffer));
  Serial.println(status_code);
  if (status_code > 0) {   
      if (status_code == HTTP_CODE_FOUND) {
        String payload = http.getString();
        Serial.println(payload);

        M5.Lcd.setCursor(10, 100);
        M5.Lcd.fillScreen(BLACK);
        M5.Lcd.setTextColor(WHITE);
        M5.Lcd.setTextSize(3);
        M5.Lcd.println("Send Done!");
      }
  } else {
      Serial.printf("[HTTP] GET... failed, error: %s\n", http.errorToString(status_code).c_str());
  }  
  http.end();
}

void loop() {
  M5.update();

  if (M5.BtnA.wasReleased()) {
    // カウントアップ
    count++;

    // ディスプレイ表示
    M5.Lcd.setCursor(10, 100);
    M5.Lcd.fillScreen(RED);
    M5.Lcd.setTextColor(YELLOW);
    M5.Lcd.setTextSize(3);
    M5.Lcd.printf("Count Up: %d", count);
  }

  if(M5.BtnC.wasReleased()) {
    // カウントダウン
    count--;

    // ゼロ以下にはしない
    if (count <= 0) count = 0;

    // ディスプレイ表示
    M5.Lcd.setCursor(10, 100);
    M5.Lcd.fillScreen(GREEN);
    M5.Lcd.setTextColor(BLACK);
    M5.Lcd.setTextSize(3);
    M5.Lcd.printf("Count Down: %d", count);
  }

  if(M5.BtnB.wasReleased()) {
    // ディスプレイ表示
    M5.Lcd.setCursor(10, 100);
    M5.Lcd.fillScreen(BLUE);
    M5.Lcd.setTextColor(WHITE);
    M5.Lcd.setTextSize(3);
    M5.Lcd.printf("Count Send: %d", count);

    // カウント送信
    sendCount();

  }
}
