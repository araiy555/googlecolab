from datetime import datetime

# 現在の日時を記録
now = datetime.now()
print(f'Task executed at: {now}')

# ファイルに記録（オプション）
with open('log.txt', 'a') as f:
    f.write(f'{now}\n')
