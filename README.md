## 安装python依赖
### pip install -r requirements.txt -i https://pypi.doubanio.com/simple


## 启动顺序

### 1. 运行 receive_quotes_client.py 接受行情 生产一分钟k线

### 2. 运行 save_ticks.py 保存一分钟k线

### 3. 运行 generate_candleline_task.py  生成多周期k线


### setting.py 中存放数据库连接信息