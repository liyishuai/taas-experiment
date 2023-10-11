import matplotlib.pyplot as plt
import numpy as np

taas_edge_color = 'xkcd:violet'
tso_edge_color = 'xkcd:rust'
taas_color = 'white'
tso_color = 'white'

plt.rc('font', size=16)
plt.margins(0)

taas_x    = [0,1,2,3,4,5]
taas_p50  = [0.2597,0.3479,0.3633,2.0644,2.6300,3.0614]
taas_p98  = [0.3572,0.3930,0.4175,4.2283,4.6447,4.9986]
tidb_x    = [0,1,2,3]
tidb_p50  = [0.1456,0.1458,1.6446,1.6402]
tidb_p98  = [0.1635,0.1629,4.1534,4.1472]

fig, (taas,tidb) = plt.subplots(1, 2, figsize=(9, 4),sharey=True,constrained_layout=True)

taas.set_title('TaaS N=5',loc='left')
taas.set_xticks(taas_x)
taas.set_xlabel('# Servers Interfered')

error_taas = taas.errorbar(taas_x,taas_p50,yerr=[taas_p98[i]-taas_p50[i] for i in taas_x],capsize=0,lolims=True,ls='None',color='orange',label='98% latency')

box_taas = taas.bar(taas_x,taas_p50,color='green',label='50% latency')
plotline_taas, caplines_taas, barlinecols_taas = error_taas
caplines_taas[0].set_marker('_')
caplines_taas[0].set_markersize(15)

tidb.set_title('TiDB-PD N=5',loc='left')

error_tidb = tidb.errorbar(tidb_x, tidb_p50, yerr=[tidb_p98[i]-tidb_p50[i] for i in tidb_x], capsize=0, lolims=True,ls='None',color='orange')
box_tidb = tidb.bar(tidb_x,tidb_p50,color='green')
plotlinet, caplinest, barlinecolst = error_tidb
caplinest[0].set_marker('_')
caplinest[0].set_markersize(15)

tidb.set_xticks(tidb_x)
tidb.set_xticklabels(['0+0','0+4','1+0','1+4'])
tidb.set_xlabel('# Leader+Followers Interfered')

#latency_ticks = [0,1,2,3,4,5]
#taas.set_ylim(0, 5)
#taas.set_yticks(latency_ticks)
#tidb.set_yticklabels(['' for _ in latency_ticks])
taas.set_ylabel('Latency (ms)')

lgd = fig.legend(handles=[box_taas,error_taas],ncol=2,bbox_to_anchor=(.78,1.15))

taas.spines['top'].set_visible(False)
tidb.spines['top'].set_visible(False)
taas.spines['right'].set_visible(False)
tidb.spines['right'].set_visible(False)
# ax.spines['right'].set_visible(False)
# ax.spines['left'].set_linewidth(2)
# ax.spines['bottom'].set_linewidth(2)

plt.rcParams['font.family'] = 'Helvetica'
fig.savefig('jitter.pdf', format='pdf',bbox_extra_artists=(lgd,), bbox_inches='tight')