import csv
import matplotlib.pyplot as plt
import numpy as np

# 从CSV文件中读取数据
data = {}
with open('latency-bar-3az.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        data[row['SYS']] = {
            'F': {
                'median': float(row['F_median']),
                'p99': float(row['F_p99'])
            },
            'G': {
                'median': float(row['G_median']),
                'p99': float(row['G_p99'])
            },
            'H': {
                'median': float(row['H_median']),
                'p99': float(row['H_p99'])
            }
        }

# 绘制柱状对比图
regions = ['F', 'G', 'H']
x = np.arange(len(regions))
width = 0.35

fig, ax = plt.subplots()

median1 = [data['TiDB-PD'][r]['median'] for r in regions]
p99_1 = [data['TiDB-PD'][r]['p99'] for r in regions]
p99_1 = [p-m for p, m in zip(p99_1, median1)]
p99_1 = (np.zeros_like(p99_1), p99_1)
rects1 = ax.bar(x - width/2, median1, width, label='TiDB-PD Median')
ax.errorbar(x - width/2, median1, yerr=p99_1, fmt='_', capsize=4, label='TiDB-PD P99')

median2 = [data['TaaS'][r]['median'] for r in regions]
p99_2 = [data['TaaS'][r]['p99'] for r in regions]
p99_2 = [p-m for p, m in zip(p99_2, median2)]
p99_2 = (np.zeros_like(p99_2), p99_2)
rects2 = ax.bar(x + width/2, median2, width, label='TaaS Median')
ax.errorbar(x + width/2, median2, yerr=p99_2, fmt='_', capsize=4, label='TaaS P99')

ax.set_ylabel('Latency (ms)')
ax.set_title('Latency in 3AZ')
ax.set_ybound(5.0)
ax.set_yticks([x * 0.5 for x in range(0, 7)])
ax.set_xticks(x)
ax.set_xticklabels(regions)
ax.legend(bbox_to_anchor=(0.99, 0.99), loc='upper right', borderaxespad=0.)

plt.subplots_adjust(hspace=0.3)
fig.subplots_adjust(top=.85)
fig.savefig('latency-bar-3az.pdf', format='pdf')