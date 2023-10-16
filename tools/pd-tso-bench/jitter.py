import matplotlib.pyplot as plt
import numpy as np
import csv

taas_edge_color = 'xkcd:violet'
tso_edge_color = 'xkcd:rust'
taas_color = 'white'
tso_color = 'white'

plt.rc('font', size=16)
plt.margins(0)

width = 0.006
x012 = np.arange(0, 6+width, width)

taas0_freq = [0 for _ in x012]
taas1_freq = [0 for _ in x012]
taas2_freq = [0 for _ in x012]

n0 = 0
n1 = 0
n2 = 0

with open('taas0.csv', encoding='utf-8-sig') as file0:
    taas0_data = csv.reader(file0)
    for row in taas0_data:
        n0 = n0 + 1
        index = int(float(row[0]) / 1000 / width)
        taas0_freq[index] = taas0_freq[index] + 1
taas0_pdf = [x / width / n0 for x in taas0_freq]

with open('taas1.csv', encoding='utf-8-sig') as file1:
    taas1_data = csv.reader(file1)
    for row in taas1_data:
        n1 = n1 + 1
        index = int(float(row[0]) / 1000 / width)
        taas1_freq[index] = taas1_freq[index] + 1
taas1_pdf = [x / width / n1 for x in taas1_freq]

with open('taas2.csv', encoding='utf-8-sig') as file2:
    taas2_data = csv.reader(file2)
    for row in taas2_data:
        n2 = n2 + 1
        index = int(float(row[0]) / 1000 / width)
        taas2_freq[index] = taas2_freq[index] + 1
taas2_pdf = [x / width / n2 for x in taas2_freq]

taas3_freq = [0, 0, 0, 9, 44, 108, 191, 250, 366, 498, 569, 662, 732, 826, 892, 988, 932, 1014, 990, 1017, 939, 925, 900, 863, 820, 682, 607, 612, 554, 430, 409, 342, 307, 265, 236, 207, 153, 132, 100, 89, 85, 69, 40, 32, 33, 25, 19, 10, 11, 6, 3, 8, 3, 1, 2, 2, 0, 0, 1, 0, 0]
taas3_pdf = [x / 2001.0 for x in taas3_freq]

taas4_freq = [0, 0, 0, 0, 3, 4, 14, 23, 69, 97, 146, 203, 302, 379, 463, 573, 616, 795, 898, 902, 943, 1012, 1030, 1052, 1042, 989, 949, 904, 851, 828, 730, 663, 554, 537, 411, 401, 309, 275, 211, 165, 162, 123, 65, 72, 43, 53, 40, 18, 15, 16, 18, 13, 7, 7, 4, 2, 5, 1, 2, 1, 1]
taas4_pdf = [x / 2001.2 for x in taas4_freq]

taas5_freq = [0, 0, 0, 0, 0, 1, 1, 1, 8, 8, 24, 33, 66, 113, 136, 221, 261, 375, 474, 534, 695, 786, 880, 883, 934, 997, 1068, 1134, 1071, 1035, 989, 936, 864, 786, 722, 703, 578, 523, 422, 388, 306, 257, 156, 117, 128, 77, 73, 51, 46, 33, 33, 14, 22, 9, 11, 4, 6, 5, 5, 0, 0]
taas5_pdf = [x / 2000.7 for x in taas5_freq]

x_ticks = np.arange(0, 6.01, 0.1)

ping_low = [0,0,386,403,391]

ping_x = np.concatenate([np.arange(0, 0.2, 0.04), np.arange(0.2, 6.01, 0.1)])

ping_high = [869,793,761,647,594,535,470,493,404,374,338,288,276,266,254,204,165,202,158,133,129,108,104,100,81,81,75,51,57,52,60,44,38,32,30,32,34,26,17,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
ping_pdf = [x / 400.0 for x in ping_low] + [x / 1000.0 for x in ping_high]

tidb_base = [20132, 53, 17, 7, 2, 1]
tidb_x = np.arange(0.1, 0.61, 0.1)
tidb_pdf = [x / 2021.2 for x in tidb_base]

tidb_interrupt = [0, 52, 168, 284, 445, 571, 659, 771, 818, 855, 871, 962, 883, 918, 924, 879, 852, 788, 700, 658, 618, 640, 575, 534, 483, 453, 440, 355, 336, 343, 242, 308, 244, 225, 196, 172, 164, 167, 168, 127, 95, 43, 2, 1, 1, 3, 1, 1, 1, 1, 0, 1, 0, 2, 0, 2, 0, 2, 0, 1, 0]
tidb_ipdf = [x / 2000.7 for x in tidb_interrupt]

# taas_x    = [0,1,2,3,4,5]
# taas_p50  = [0.2597,0.3479,0.3633,2.0644,2.6300,3.0614]
# taas_p98  = [0.3572,0.3930,0.4175,4.2283,4.6447,4.9986]
# tidb_x    = [0,1,2,3]
# tidb_p50  = [0.1456,0.1458,1.6446,1.6402]
# tidb_p98  = [0.1635,0.1629,4.1534,4.1472]

fig, (taas012, taas345,tidb) = plt.subplots(1, 3, figsize=(12, 4),sharex=False,sharey=False,constrained_layout=True)

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

upperi = 0.43
upper012 = int(upperi/width)+1

taas012.plot(x012[:upper012], taas0_pdf[:upper012], ':g', label='all intact')
taas012.plot(x012[:upper012], taas1_pdf[:upper012], dashes=[5, 1, 1, 1, 1, 1, 1, 1],color='y', label='1/5 interfered')
taas012.plot(x012[:upper012], taas2_pdf[:upper012], dashes=[5, 2, 1, 2, 1, 2], color='c', label='2/5 interfered')

taas012.set_ylim(0, 22)
taas012.set_xlim(0, upperi)
taas012.set_xticks(np.arange(0, upperi+width, 0.1))
taas012.legend()

taas345.set_ylim(0,1.1)
taas345.set_yticks(np.arange(0, 1.1, 0.2))
taas345.set_xlim(0,6)
taas345.set_xticks([0,1,2,3,4,5,6])

lower0 = int(0.2/width)
lower12 = int(0.35/width)
upper345 = int(1/width)

# taas345.plot(x012[lower0:], taas0_pdf[lower0:], ':g')
# taas345.plot(x012[lower12:], taas1_pdf[lower12:], dashes=[5, 1, 1, 1, 1, 1, 1, 1],color='y')
taas345.plot(x012[lower12:], taas2_pdf[lower12:], dashes=[5, 2, 1, 2, 1, 2], color='c')
taas345.plot(x_ticks, taas3_pdf, '-.b', label='3/5 interfered')
taas345.plot(x_ticks, taas4_pdf, '--m', label='4/5 interfered')
taas345.plot(x_ticks, taas5_pdf, '-r', label='all interfered')
taas345.legend()

ping_lower=3

tidb.set_ylim(0,1.1)
tidb.set_yticks(np.arange(0, 1.1, 0.2))
tidb.set_xlim(0,6)
tidb.set_xticks([0,1,2,3,4,5,6])
tidb.plot(tidb_x, tidb_pdf, '-.g', label='leader intact')
tidb.plot(x_ticks, tidb_ipdf, '-r', label='leader interfered')
tidb.plot(ping_x[ping_lower:], ping_pdf[ping_lower:], ':k',label='interfered ping')
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
