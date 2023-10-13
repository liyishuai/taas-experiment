import matplotlib.pyplot as plt
import numpy as np

taas_edge_color = 'xkcd:violet'
tso_edge_color = 'xkcd:rust'
taas_color = 'white'
tso_color = 'white'

plt.rc('font', size=16)
plt.margins(0)

taas0_freq = [16264,2444,65,32,12,7,5,1,1]
taas0_x = np.arange(0.2, 1.01, 0.1)
taas0_pdf = [x / 2010.2 for x in taas0_freq]

taas1_freq = [19855,125,42,33,25,7,6,3,1,0,1,0,2,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1]
taas1_x = np.arange(0.3, 5.61, 0.1)
taas1_pdf = [x / 2011.7 for x in taas1_freq]

taas2_freq = [19377,412,68,52,37,18,13,19,14,16,7,0,1,0,1,1,1,0,0,2,0,0,0,1,0,0,0,0,2,0,1]
taas2_x = np.arange(0.3, 3.31, 0.1)
taas2_pdf = [x / 2004.6 for x in taas2_freq]

taas3_freq = [0,0,0,12,59,95,196,254,329,431,577,599,711,750,835,931,969,923,927,888,908,861,862,734,755,734,649,600,544,510,428,365,373,287,263,254,219,170,129,151,117,172,79,63,58,49,36,33,23,23,19,11,14,12,6,8,3,2,0,0,2]
taas3_pdf = [x / 2001.7 for x in taas3_freq]

taas4_freq = [0,0,0,1,2,2,16,28,58,90,110,159,227,301,365,454,559,622,712,801,846,879,950,900,879,880,876,891,817,786,756,677,650,600,534,491,420,378,333,299,243,484,196,116,126,76,86,68,50,39,50,22,26,21,15,10,8,4,3,7,3]
taas4_pdf = [x / 2000.7 for x in taas4_freq]

taas5_freq = [0,0,0,0,0,0,0,3,4,8,12,36,45,82,108,152,193,260,338,409,515,593,677,734,814,890,935,953,936,997,995,863,804,835,802,727,657,568,530,520,445,885,350,209,186,160,129,124,89,87,78,56,43,31,29,22,17,17,14,6,8]
taas5_pdf = [x / 2000.4 for x in taas5_freq]

x_ticks = np.arange(0, 6.01, 0.1)

ping_freq = [358,9444,8616,7915,7047,6363,5734,5188,4751,4216,3853,3872,3211,3065,2529,2304,2291,1894,1911,1605,1398,1278,1229,1001,945,884,771,781,659,576,503,478,468,372,360,315,271,296,265,207,1860,181,4,1,0,0,0,0,0,0,0,0,0,0,      0,0,0,0,0,0,0]
ping_pdf = [x / 10000.0 for x in ping_freq]

tidb_base = [20010, 16, 3, 3, 1, 0, 1]
tidb_x = np.arange(0.1, 0.71, 0.1)
tidb_pdf = [x / 2003.4 for x in tidb_base]

tidb_interrupt = [0,59,169,287,419,537,637,731,798,840,880,901,839,846,855,801,774,724,717,649,596,607,555,437,481,415,418,359,317,285,242,250,230,219,198,175,172,162,128,118,106,1068,3,0,3,3,0,1,0,3,1,1,1,1,0,0,0,0,0,1,0]
tidb_ipdf = [x / 2002.0 for x in tidb_interrupt]

# taas_x    = [0,1,2,3,4,5]
# taas_p50  = [0.2597,0.3479,0.3633,2.0644,2.6300,3.0614]
# taas_p98  = [0.3572,0.3930,0.4175,4.2283,4.6447,4.9986]
# tidb_x    = [0,1,2,3]
# tidb_p50  = [0.1456,0.1458,1.6446,1.6402]
# tidb_p98  = [0.1635,0.1629,4.1534,4.1472]

fig, (taas,tidb) = plt.subplots(1, 2, figsize=(9, 4),sharex=True,sharey=True,constrained_layout=True)

taas.set_title('TaaS N=5',loc='left')
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

taas.set_ylim(0,1)
taas.set_xlim(0,6)
taas.set_xticks([0,1,2,3,4,5,6])
taas.set_ylabel('Density ($\mathrm{ms}^{-1}$)')

taas.plot(taas0_x, taas0_pdf, ':g', label='0/5')
taas.plot(taas1_x, taas1_pdf, dashes=[5, 1, 1, 1, 1, 1, 1, 1],color='y', label='1/5')
taas.plot(taas2_x, taas2_pdf, dashes=[5, 1, 1, 1, 1, 1], color='c', label='2/5')
taas.plot(x_ticks, taas3_pdf, '-.b', label='3/5')
taas.plot(x_ticks, taas4_pdf, '--m', label='4/5')
taas.plot(x_ticks, taas5_pdf, '-r', label='5/5')
taas.legend(bbox_to_anchor=(.7, 1.1))

tidb.plot(tidb_x, tidb_pdf, '-.g', label='leader intact')
tidb.plot(x_ticks, tidb_ipdf, '-r', label='leader interfered')
tidb.plot(x_ticks, ping_pdf, ':k',label='interfered ping')
tidb.legend()

# lgd = fig.legend(handles=[box_taas,error_taas],ncol=2,bbox_to_anchor=(.78,1.15))

taas.spines['top'].set_visible(False)
tidb.spines['top'].set_visible(False)
taas.spines['right'].set_visible(False)
tidb.spines['right'].set_visible(False)
# ax.spines['right'].set_visible(False)
# ax.spines['left'].set_linewidth(2)
# ax.spines['bottom'].set_linewidth(2)

xlabel = fig.supxlabel('Latency (ms)')

plt.rcParams['font.family'] = 'Helvetica'
fig.savefig('jitter.pdf', format='pdf', bbox_inches='tight', bbox_extra_artists=(xlabel,))
