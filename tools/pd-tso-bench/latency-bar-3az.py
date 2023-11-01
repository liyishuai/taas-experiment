import csv
import matplotlib.pyplot as plt
import numpy as np

taas_edge_color = 'c'
taasfg_edge_color = 'xkcd:violet'
tso_edge_color = 'xkcd:rust'

taas_color = 'white'
tso_color = 'white'
taasfg_color = 'white'

marker_size=14

plt.rc('font', size=16)
plt.margins(0)

tidb_p50   = [0.1474, 0.9833, 1.4241]
tidb_p99   = [0.2136, 1.1437, 1.5459]
taas_p50   = [1.4556, 1.1261, 1.1828]
taas_p99   = [3.2273, 4.1041, 4.1865]
taasfg_p50 = [2.5398, 1.5146, 3.3182]
taasfg_p99 = [9.5277, 7.6771, 8.7636]


# 绘制柱状对比图
regions = ['Beijing F', 'Beijing G', 'Beijing H']
width = 0.75

fig, ax = plt.subplots(figsize=(9, 4))

tidb_x   = [1, 5, 9]
taas_x   = [2, 6, 10]
taasfg_x = [3, 7, 11]

tidb = ax.bar(tidb_x, tidb_p50, width, label='TiDB-PD', color=tso_color, hatch='---', edgecolor=tso_edge_color)
plotline_tidb, caplines_tidb, barlinecols_tidb = ax.errorbar(tidb_x, tidb_p50, yerr=[tidb_p99[0]-tidb_p50[0], tidb_p99[1]-tidb_p50[1], tidb_p99[2]-tidb_p50[2]], capsize=0, lolims=True, ls='None', color='black')
caplines_tidb[0].set_marker('_')
caplines_tidb[0].set_markersize(marker_size)

taas = ax.bar(taas_x, taas_p50, width, label='TaaS (intact)', color=taas_color, hatch='////', edgecolor=taas_edge_color)
plotline_taas, caplines_taas, barlinecols_taas = ax.errorbar(taas_x, taas_p50, yerr=[taas_p99[0]-taas_p50[0], taas_p99[1]-taas_p50[1], taas_p99[2]-taas_p50[2]], capsize=0, lolims=True, ls='None', color='black')
caplines_taas[0].set_marker('_')
caplines_taas[0].set_markersize(marker_size)

taasfg = ax.bar(taasfg_x, taasfg_p50, width, label='TaaS (H outage)', color=taasfg_color, hatch='\\\\\\\\', edgecolor=taasfg_edge_color)
plotline_taasfg, caplines_taasfg, barlinecols_taasfg = ax.errorbar(taasfg_x, taasfg_p50, yerr=[taasfg_p99[0]-taasfg_p50[0], taasfg_p99[1]-taasfg_p50[1], taasfg_p99[2]-taasfg_p50[2]], capsize=0, lolims=True, ls='None', color='black')
caplines_taasfg[0].set_marker('_')
caplines_taasfg[0].set_markersize(marker_size)

ax.set_ylabel('Latency (ms)')
# ax.set_title('Latency in 3AZ')
ax.set_yscale('log')
# ax.set_ylim(0.1, 2.1)
ax.set_yticks([.1, .2, 1, 2, 4, 10])
ax.set_yticklabels(["%g" % x for x in ax.get_yticks()])
# ax.set_yticks([.1, .2, .3, 1, 1.5, 2])
# ax.set_yticklabels(['0.1', '0.2', '0.3', '1.0', '1.5', '2.0'])
ax.set_xticks(taas_x)
ax.set_xticklabels(regions)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['left'].set_linewidth(2)
ax.spines['bottom'].set_linewidth(2)
ax.tick_params(axis='both', which='both', width=2, length=3)
ax.tick_params(axis='both', which='major', width=2, length=6)
# ax.legend(ncol=3, bbox_to_anchor=(0.99, 0.99), loc='upper right', borderaxespad=0.)
ax.legend(ncol=3, loc='upper center', bbox_to_anchor=(.45, 1.18))

# ax.spines['top'].set_visible(False)
# ax.spines['right'].set_visible(False)

# plt.subplots_adjust(left=0.1, right=0.99, bottom=0.08, top=0.98)
plt.rcParams['font.family'] = 'Helvetica'
fig.savefig('latency-bar-3az.pdf', format='pdf', bbox_inches='tight')