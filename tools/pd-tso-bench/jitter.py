import matplotlib.pyplot as plt
import numpy as np
import csv

taas_edge_color = 'xkcd:violet'
tso_edge_color = 'xkcd:rust'
taas_color = 'white'
tso_color = 'white'

plt.rc('font', size=16)
plt.margins(0)

width0 = 0.006
x0 = np.arange(0, 6 + width0, width0)

width1 = 0.005
x1 = np.arange(0, 6 + width1, width1)

width2a = 0.003
x2a = np.arange(0, 6+width2a, width2a)

width2b = 0.06
x2b = np.arange(0, 6 + width2b, width2b)

width3 = 0.2
x3 = np.arange(0, 6 + width3, width3)

width4 = 0.2
x4 = np.arange(0, 6 + width4, width4)

width5 = 0.2
x5 = np.arange(0, 6+width5, width5)

taas0_freq = [0 for _ in x0]
taas1_freq = [0 for _ in x1]
taas2a_freq = [0 for _ in x2a]
taas2b_freq = [0 for _ in x2b]
taas3_freq = [0 for _ in x3]
taas4_freq = [0 for _ in x4]
taas5_freq = [0 for _ in x5]

n0 = 0
n1 = 0
n2 = 0
n3 = 0
n4 = 0
n5 = 0

with open('taas0.csv', encoding='utf-8-sig') as file0:
    taas0_data = csv.reader(file0)
    for row in taas0_data:
        n0 = n0 + 1
        index = int(float(row[0]) / 1000 / width0)
        taas0_freq[index] = taas0_freq[index] + 1
taas0_pdf = [x / width0 / n0 for x in taas0_freq]

with open('taas1.csv', encoding='utf-8-sig') as file1:
    taas1_data = csv.reader(file1)
    for row in taas1_data:
        n1 = n1 + 1
        index = int(float(row[0]) / 1000 / width1)
        taas1_freq[index] = taas1_freq[index] + 1
taas1_pdf = [x / width1 / n1 for x in taas1_freq]

with open('taas2.csv', encoding='utf-8-sig') as file2:
    taas2_data = csv.reader(file2)
    for row in taas2_data:
        n2 = n2 + 1
        index_a = int(float(row[0]) / 1000 / width2a)
        taas2a_freq[index_a] = taas2a_freq[index_a] + 1
        index_b = int(float(row[0]) / 1000 / width2b)
        taas2b_freq[index_b] = taas2b_freq[index_b] + 1
taas2a_pdf = [x / width2a / n2 for x in taas2a_freq]
taas2b_pdf = [x / width2b / n2 for x in taas2b_freq]

with open('taas3.csv', encoding='utf-8-sig') as file3:
    taas3_data = csv.reader(file3)
    for row in taas3_data:
        n3 = n3 + 1
        index = int(float(row[0]) / 1000 / width3)
        if index < len(taas3_freq):
            taas3_freq[index] = taas3_freq[index] + 1
taas3_pdf = [x / width3 / n3 for x in taas3_freq]

with open('taas4.csv', encoding='utf-8-sig') as file4:
    taas4_data = csv.reader(file4)
    for row in taas4_data:
        n4 = n4 + 1
        index = int(float(row[0]) / 1000 / width4)
        if index < len(taas4_freq):
            taas4_freq[index] = taas4_freq[index] + 1
taas4_pdf = [x / width4 / n4 for x in taas4_freq]

with open('taas5.csv', encoding='utf-8-sig') as file5:
    taas5_data = csv.reader(file5)
    for row in taas5_data:
        n5 = n5 + 1
        index = int(float(row[0]) / 1000 / width5)
        if index < len(taas5_freq):
            taas5_freq[index] = taas5_freq[index] + 1
taas5_pdf = [x / width5 / n5 for x in taas5_freq]

ping_width = 0.09
ping_x = np.arange(0, 6 + ping_width, ping_width)
ping_freq = [0 for _ in ping_x]
ping_n = 0

with open('ping1.csv', encoding='utf-8-sig') as file_ping:
    ping_data = csv.reader(file_ping)
    for row in ping_data:
        ping_n = ping_n + 1
        index = int(float(row[0]) / ping_width)
        ping_freq[index] = ping_freq[index] + 1

ping_pdf = [x / ping_width / ping_n for x in ping_freq]

tidb0_width = 0.005
tidb0_x = np.arange(0, 6 + tidb0_width, tidb0_width)
tidb_base = [0 for _ in tidb0_x]
tidb0_n = 0

tidb1_width = 0.09
tidb1_x = np.arange(0, 6 + tidb1_width, tidb1_width)
tidb_interrupt = [0 for _ in tidb1_x]
tidb1_n = 0

with open('tidb0.csv', encoding='utf-8-sig') as file_tidb0:
    tidb0_data = csv.reader(file_tidb0)
    for row in tidb0_data:
        tidb0_n = tidb0_n + 1
        index = int(float(row[0]) / 1000 / tidb0_width)
        if index < len(tidb_base):
            tidb_base[index] = tidb_base[index] + 1

tidb_pdf = [x / tidb0_width / tidb0_n for x in tidb_base]

with open('tidb1.csv', encoding='utf-8-sig') as file_tidb1:
    tidb1_data = csv.reader(file_tidb1)
    for row in tidb1_data:
        tidb1_n = tidb1_n + 1
        index = int(float(row[0]) / 1000 / tidb1_width)
        if index < len(tidb_interrupt):
            tidb_interrupt[index] = tidb_interrupt[index] + 1
tidb_ipdf = [x / tidb1_width / tidb1_n for x in tidb_interrupt]

# taas_x    = [0,1,2,3,4,5]
# taas_p50  = [0.2597,0.3479,0.3633,2.0644,2.6300,3.0614]
# taas_p98  = [0.3572,0.3930,0.4175,4.2283,4.6447,4.9986]
# tidb_x    = [0,1,2,3]
# tidb_p50  = [0.1456,0.1458,1.6446,1.6402]
# tidb_p98  = [0.1635,0.1629,4.1534,4.1472]

fig, (taas012, taas345,tidb) = plt.subplots(1, 3, figsize=(14, 4),sharex=False,sharey=False,constrained_layout=True)

taas012.set_title('TaaS N=5',loc='left')
# taas.set_xticks(taas_x)
# taas.set_xlabel('# Servers Interfered')

# error_taas = taas.errorbar(taas_x,taas_p50,yerr=[taas_p98[i]-taas_p50[i] for i in taas_x],capsize=0,lolims=True,ls='None',color='orange',label='98% latency')

# box_taas = taas.bar(taas_x,taas_p50,color='green',label='50% latency')
# plotline_taas, caplines_taas, barlinecols_taas = error_taas
# caplines_taas[0].set_marker('_')
# caplines_taas[0].set_markersize(15)

tidb.set_title('TiDB-PD N=5',loc='left')

# error_tidb = tidb.errorbar(tidb_x, tidb_p50, yerr=[tidb_p98[i]-tidb_p50[i] for i in tidb_x], capsize=0, lolims=True,ls='None',color='orange')
# box_tidb = tidb.bar(tidb_x,tidb_p50,color='green')
# plotlinet, caplinest, barlinecolst = error_tidb
# caplinest[0].set_marker('_')
# caplinest[0].set_markersize(15)

# tidb.set_xticks(tidb_x)
# tidb.set_xticklabels(['0+0','0+4','1+0','1+4'])
# tidb.set_xlabel('# Leader+Followers Interfered')

#latency_ticks = [0,1,2,3,4,5]
#taas.set_ylim(0, 5)
#taas.set_yticks(latency_ticks)
#tidb.set_yticklabels(['' for _ in latency_ticks])
# taas.set_ylabel('Latency (ms)')


taas012.set_ylabel('Density ($\mathrm{ms}^{-1}$)')

taas012.plot(x0, taas0_pdf, ':g', label='all intact')
taas012.plot(x1, taas1_pdf, dashes=[5, 1, 1, 1, 1, 1, 1, 1],color='y', label='1/5 interfered')
taas012.plot(x2a, taas2a_pdf, dashes=[5, 2, 1, 2, 1, 2], color='c', label='2/5 interfered')

upperi = 0.43
taas012.set_ylim(0)
taas012.set_xlim(0, upperi)
# taas012.set_xticks(np.arange(0, upperi+0.01, 0.1))
taas012.legend()

taas345.set_ylim(0,1.1)
# taas345.set_yticks(np.arange(0, 1.1, 0.5))
taas345.set_xlim(0,6)
taas345.set_xticks([0,1,2,3,4,5,6])

taas345.plot(x2b, taas2b_pdf, dashes=[5, 2, 1, 2, 1, 2], color='c')
taas345.plot(x3, taas3_pdf, '-.b', label='3/5 interfered')
taas345.plot(x4, taas4_pdf, '--m', label='4/5 interfered')
taas345.plot(x5, taas5_pdf, '-r', label='all interfered')
taas345.legend()

tidb.set_ylim(0,1.1)
# tidb.set_yticks(np.arange(0, 1.1, 0.5))
tidb.set_xlim(0,6)
tidb.set_xticks([0,1,2,3,4,5,6])
tidb.plot(tidb0_x, tidb_pdf, '-.g', label='leader intact')
tidb.plot(tidb1_x, tidb_ipdf, '-r', label='leader interfered')
tidb.plot(ping_x, ping_pdf, ':k',label='interfered ping')
tidb.legend()

# lgd = fig.legend(handles=[box_taas,error_taas],ncol=2,bbox_to_anchor=(.78,1.15))

taas012.spines['top'].set_visible(False)
taas345.spines['top'].set_visible(False)
tidb.spines['top'].set_visible(False)
taas012.spines['right'].set_visible(False)
taas345.spines['right'].set_visible(False)
tidb.spines['right'].set_visible(False)
# ax.spines['right'].set_visible(False)
# ax.spines['left'].set_linewidth(2)
# ax.spines['bottom'].set_linewidth(2)

xlabel = fig.supxlabel('Latency (ms)')

plt.rcParams['font.family'] = 'Helvetica'
fig.savefig('jitter.pdf', format='pdf', bbox_inches='tight', bbox_extra_artists=(xlabel,))
