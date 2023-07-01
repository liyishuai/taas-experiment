import csv
import matplotlib.pyplot as plt
import numpy as np

taas_edge_color = 'xkcd:violet'
tso_edge_color = 'xkcd:rust'
taas_color = 'white'
tso_color = 'white'
bar_skew = 0.04

plt.rc('font', size=16)
plt.margins(0)

# 从CSV文件中读取数据
data = {}
with open('latency-bar-3az.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        data[row['SYS']] = {
            'Beijing F': {
                'median': float(row['F_median']),
                'p99': float(row['F_p99'])
            },
            'Beijing G': {
                'median': float(row['G_median']),
                'p99': float(row['G_p99'])
            },
            'Beijing H': {
                'median': float(row['H_median']),
                'p99': float(row['H_p99'])
            }
        }

# 绘制柱状对比图
regions = ['Beijing F', 'Beijing G', 'Beijing H']
x = np.arange(len(regions))
width = 0.2

fig, ax = plt.subplots(figsize=(9, 4))

median1 = [data['TiDB-PD'][r]['median'] for r in regions]
p99_1 = [data['TiDB-PD'][r]['p99'] for r in regions]
p99_1 = [p-m for p, m in zip(p99_1, median1)]
p99_1 = (np.zeros_like(p99_1), p99_1)
rects1 = ax.bar(x - width/2 - bar_skew, median1, width, label='TiDB-PD', color=tso_color, hatch='////', edgecolor=tso_edge_color)
plotline1, caplines1, barlinecols1 = ax.errorbar(x - width/2 -bar_skew, median1, yerr=p99_1, capsize=0, lolims=True, ls='None', color='black')
caplines1[0].set_marker('_')
caplines1[0].set_markersize(12)

median2 = [data['TaaS'][r]['median'] for r in regions]
p99_2 = [data['TaaS'][r]['p99'] for r in regions]
p99_2 = [p-m for p, m in zip(p99_2, median2)]
p99_2 = (np.zeros_like(p99_2), p99_2)
rects2 = ax.bar(x + width/2 + bar_skew, median2, width, label='TaaS', color=taas_color, hatch="\\\\\\\\", edgecolor=taas_edge_color)
plotline2, caplines2, barlinecols2 = ax.errorbar(x + width/2 + bar_skew, median2, yerr=p99_2, capsize=0, lolims=True, ls='None', color='black')
caplines2[0].set_marker('_')
caplines2[0].set_markersize(12)

ax.set_ylabel('Latency (ms)')
# ax.set_title('Latency in 3AZ')
ax.set_yscale('log')
ax.set_ylim(0.1, 2.1)
ax.set_yticks([.1, .2, .3, 1, 1.5, 2])
ax.set_yticklabels(['0.1', '0.2', '0.3', '1.0', '1.5', '2.0'])
ax.set_xticks(x)
ax.set_xticklabels(regions)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['left'].set_linewidth(2)
ax.spines['bottom'].set_linewidth(2)
ax.tick_params(axis='both', which='both', width=2, length=3)
ax.tick_params(axis='both', which='major', width=2, length=6)
# ax.legend(ncol=3, bbox_to_anchor=(0.99, 0.99), loc='upper right', borderaxespad=0.)
ax.legend(ncol=3, loc='upper center')

# ax.spines['top'].set_visible(False)
# ax.spines['right'].set_visible(False)

plt.subplots_adjust(left=0.1, right=0.99, bottom=0.08, top=0.98)
plt.rcParams['font.family'] = 'Helvetica'
fig.savefig('latency-bar-3az.pdf', format='pdf')