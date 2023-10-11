import matplotlib.pyplot as plt
import numpy as np

taas_edge_color = 'xkcd:violet'
tso_edge_color = 'xkcd:rust'
taas_color = 'white'
tso_color = 'white'
width=1

plt.rc('font', size=16)
plt.margins(0)

x          = [3     ,5     ,7     ,9     ]
xlabels    = ['N=3' ,'N=5' ,'N=7' ,'N=9' ]
taas_loads = [1.11  ,1.75  ,2.47  ,3.33  ]
taas_p50   = [0.2297,0.2558,0.2721,0.2915]
taas_p99   = [0.3376,0.3704,0.3901,0.4183]
tidb_loads = [0.39  ,0.43  ,0.38  ,0.41  ]
tidb_p50   = [0.1462,0.1516,0.1451,0.154 ]
tidb_p99   = [0.1649,0.1738,0.1739,0.1766]

fig, (taas_load, tidb_load) = plt.subplots(1, 2, figsize=(9, 4),sharey=True,sharex=True)

taas_latency = taas_load.twinx()
tidb_latency = tidb_load.twinx()

error1 = taas_latency.errorbar(x, taas_p50, yerr=[taas_p99[i]-taas_p50[i] for i in [0,1,2,3]], capsize=0, lolims=True,ls='None',label='99% latency',color='orange')
box = taas_latency.bar(x,taas_p50,width,label='50% latency',color='green')
plotline1, caplines1, barlinecols1 = error1
caplines1[0].set_marker('_')
caplines1[0].set_markersize(20)

taas_load.set_title('TaaS',loc='left')
taas_load.set_xlim(2,10)
taas_load.set_xticks(x)
taas_load.set_xticklabels(xlabels)

line = taas_load.plot(x, taas_loads, 'o:',color='black', label='load')
taas_load.set_ylim(0,4)
taas_load.set_ylabel('CPU Load')
taas_load.set_zorder(taas_latency.zorder+1)
taas_load.set_frame_on(False)


tidb_load.set_title('TiDB-PD',loc='left')
tidb_load.set_xticks(x)
tidb_load.set_xticklabels(xlabels)
tidb_load.set_xlim(2,10)

tidb_load.plot(x, tidb_loads, 'o:', color='black')

tidb_latency.bar(x,tidb_p50,width,color='green')
plotline2, caplines2, barlinecols2 = tidb_latency.errorbar(x, tidb_p50, yerr=[tidb_p99[i]-tidb_p50[i] for i in [0,1,2,3]], capsize=0, lolims=True,ls='None',color='orange')
caplines2[0].set_marker('_')
caplines2[0].set_markersize(20)

#tidb_latency.set_yscale('log')
#taas_latency.set_yscale('log')
#tidb_latency.set_ylim(0,0.5)
#taas_latency.set_ylim(0,0.5)

latency_ticks = [0,0.1,0.2,0.3,0.4,0.5]
taas_latency.set_yticks(latency_ticks)
tidb_latency.set_yticks(latency_ticks)
taas_latency.set_yticklabels(['' for _ in latency_ticks])
tidb_latency.set_yticklabels(['%.1f' % x for x in latency_ticks])

#taas_latency.set_yticklabels(['' for _ in latency_ticks])

tidb_latency.set_ylabel('Latency (ms)')

tidb_load.set_zorder(tidb_latency.zorder+1)
tidb_load.set_frame_on(False)
fig.legend(handles=[line[0], box, error1],bbox_to_anchor=(.9, .9))

taas_load.spines['top'].set_visible(False)
tidb_load.spines['top'].set_visible(False)
taas_latency.spines['top'].set_visible(False)
tidb_latency.spines['top'].set_visible(False)
# ax.spines['right'].set_visible(False)
# ax.spines['left'].set_linewidth(2)
# ax.spines['bottom'].set_linewidth(2)

plt.rcParams['font.family'] = 'Helvetica'
fig.savefig('load-node.pdf', format='pdf')