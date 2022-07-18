#include <M5Stack.h>

// カウント初期化
int count = 0;

void setup() {
  // m5stackを初期化
  M5.begin();
  // Lcd ディスプレイにアクセスするためのクラス
  M5.Lcd.clear(BLACK);
  M5.Lcd.setTextSize(3);
  M5.Lcd.setCursor(10, 100);
  M5.Lcd.println("Button Click!");
}

// Add the main program code into the continuous loop() function
void loop() {
  // m5stack のボタン状態を更新
  // https://lang-ship.com/reference/unofficial/M5StickC/Class/Button/
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
}
