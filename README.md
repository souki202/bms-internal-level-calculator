# 譜面定数計算機

orajaのsongdata.dbを参照し、BMSの譜面定数を計算するためのツール
精度は多分イマイチ

## 使い方

Node.jsが必要です

1. DEFAULT_SONGDATA_DB_PATHを適切なパスに書き換える
2. `npm i`で依存パッケージをインストール
3. `npm start`で起動
4. 出力された`classified.json`の"folder"全体を、beatorajaのtable/default.jsonのfolderにコピペ

table/default.jsonに値を設定済みの場合は、元あったデータを上書きしないように注意してください
