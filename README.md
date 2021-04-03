# stock-tracker

## 環境架設

1. 安裝 Python 3.8.X 的版本
2. 安裝 Pipenv

```
$ pip install pipenv
$ pipenv --three  // 產生 Python 3 虛擬環境
$ pipenv install --dev  // 安裝依賴
```

如果跳出 ValueError: unknown locale: UTF-8 相關錯誤，表示系統需要設定 LANG 環境變數 e.g.

```
export LANG=en_US.UTF-8 // ~/.bashrc or ~/.zshrc
```


### 安裝套件
```
$ pipenv install requests
$ pipenv install pytest --dev  // development 環境
```


### 如何跑 spider
1. 到要跑 Spider 的目錄下輸入 `tutorial/tutorial/spiders/`:
```
scrapy crawl quotes
```

