import matplotlib.pyplot as plt
import numpy as np

taas_edge_color = 'xkcd:violet'
tso_edge_color = 'xkcd:rust'
taas_color = 'white'
tso_color = 'white'

plt.rc('font', size=16)
plt.margins(0)


throughput_ticks = [3000, 5000, 10000, 30000, 70000]
throughput_labels = ['3k','5k','10k','30k','70k']

x          = [1,2,4,8,16]
xlabels    = ['1','2','4','8','16']
taas3_rate = [4907,7949,14845,25186,40108]
taas3_p50  = [0.185,0.2448,0.2579,0.293,0.3459]
taas3_p99  = [0.3438,0.3571,0.3748,0.4351,0.523]
taas5_rate = [3888,7192,12870,21236,32230]
taas5_p50 = [0.2525,0.2719,0.2998,0.3538,0.4562]
taas5_p99 = [0.3671,0.3892,0.4274,0.5023,0.7248]
taas7_rate = [3577,6574,11469,18673,25636]
taas7_p50 = [0.2762,0.2983,0.3385,0.4102,0.5711]
taas7_p99 = [0.3939,0.427,0.4801,0.5911,1.0228]
taas9_rate = [3354,6230,10507,16423,21555]
taas9_p50 = [0.2937,0.3158,0.3698,0.4669,0.6792]
taas9_p99 = [0.4221,0.4512,0.5283,0.6962,1.3214]
tidb_rate = [6611,12700,23840,39460,69315]
tidb_p50  = [0.1454,0.1484,0.1538,0.1677,0.1963]
tidb_p99  = [0.1642,0.1802,0.1932,0.2155,0.2679]

fig, (taas3,taas5,taas7,taas9,tidb) = plt.subplots(1, 5, figsize=(13, 4),sharey=True,sharex=True,constrained_layout=True)

taas3_latency = taas3.twinx()
taas5_latency = taas5.twinx()
taas7_latency = taas7.twinx()
taas9_latency = taas9.twinx()
tidb_latency = tidb.twinx()

error3 = taas3_latency.errorbar(x, taas3_p50, yerr=[taas3_p99[i]-taas3_p50[i] for i in [0,1,2,3,4]], capsize=0, lolims=True,ls='None',label='99% latency',color='orange')
box3 = taas3_latency.bar(x,taas3_p50,label='50% latency',color='green',width=[xx * 0.4 for xx in x])
plotline3, caplines3, barlinecols3 = error3
caplines3[0].set_marker('_')
caplines3[0].set_markersize(15)

taas3.set_title('TaaS N=3',loc='left')
taas3.set_xscale('log')
taas3.set_xticks(x)
taas3.set_xticklabels(xlabels)
taas3.minorticks_off()

line3 = taas3.plot(x, taas3_rate,'o:', color='black', label='throughput')
taas3.set_ylim(3000,75000)
taas3.set_yscale('log')
taas3.set_yticks(throughput_ticks)
taas3.set_yticklabels(throughput_labels)
taas3.set_ylabel('Machine Throughput')
taas3.set_zorder(taas3_latency.zorder+1)
taas3.set_frame_on(False)

taas5.set_title('TaaS N=5',loc='left')

error5 = taas5_latency.errorbar(x, taas5_p50, yerr=[taas5_p99[i]-taas5_p50[i] for i in [0,1,2,3,4]], capsize=0, lolims=True,ls='None',color='orange')
box5 = taas5_latency.bar(x,taas5_p50,color='green',width=[xx * 0.4 for xx in x])
plotline5, caplines5, barlinecols5 = error5
caplines5[0].set_marker('_')
caplines5[0].set_markersize(15)

taas5.plot(x, taas5_rate,'o:',color='black')
taas5.set_zorder(taas5_latency.zorder+1)
taas5.set_frame_on(False)

taas7.set_title('TaaS N=7',loc='left')

error7 = taas7_latency.errorbar(x, taas7_p50, yerr=[taas7_p99[i]-taas7_p50[i] for i in [0,1,2,3,4]], capsize=0, lolims=True,ls='None',color='orange')
box5 = taas7_latency.bar(x,taas7_p50,color='green',width=[xx * 0.4 for xx in x])
plotline7, caplines7, barlinecols7 = error7
caplines7[0].set_marker('_')
caplines7[0].set_markersize(15)

taas7.plot(x, taas7_rate,'o:',color='black')
taas7.set_zorder(taas7_latency.zorder+1)
taas7.set_frame_on(False)

taas9.set_title('TaaS N=9',loc='left')

error9 = taas9_latency.errorbar(x, taas9_p50, yerr=[taas9_p99[i]-taas9_p50[i] for i in [0,1,2,3,4]], capsize=0, lolims=True,ls='None',color='orange')
box5 = taas9_latency.bar(x,taas9_p50,color='green',width=[xx * 0.4 for xx in x])
plotline9, caplines9, barlinecols9 = error9
caplines9[0].set_marker('_')
caplines9[0].set_markersize(15)

taas9.plot(x, taas9_rate,'o:',color='black')
taas9.set_zorder(taas9_latency.zorder+1)
taas9.set_frame_on(False)

tidb.set_title('TiDB-PD N=9',loc='left')

errort = tidb_latency.errorbar(x, tidb_p50, yerr=[tidb_p99[i]-tidb_p50[i] for i in [0,1,2,3,4]], capsize=0, lolims=True,ls='None',color='orange')
box5 = tidb_latency.bar(x,tidb_p50,color='green',width=[xx * 0.4 for xx in x])
plotlinet, caplinest, barlinecolst = errort
caplinest[0].set_marker('_')
caplinest[0].set_markersize(15)

tidb.plot(x, tidb_rate,'o:',color='black')
tidb.set_zorder(tidb_latency.zorder+1)
tidb.set_frame_on(False)

latency_ticks = [0, 0.5, 1.0, 1.5]
tidb_latency.set_ylim(0, 1.5)
taas3_latency.set_ylim(0, 1.5)
taas5_latency.set_ylim(0, 1.5)
taas7_latency.set_ylim(0, 1.5)
taas9_latency.set_ylim(0, 1.5)
tidb_latency.set_yticks(latency_ticks)
taas3_latency.set_yticks(latency_ticks)
taas5_latency.set_yticks(latency_ticks)
taas7_latency.set_yticks(latency_ticks)
taas9_latency.set_yticks(latency_ticks)
taas3_latency.set_yticklabels(['' for _ in latency_ticks])
taas5_latency.set_yticklabels(['' for _ in latency_ticks])
taas7_latency.set_yticklabels(['' for _ in latency_ticks])
taas9_latency.set_yticklabels(['' for _ in latency_ticks])
tidb_latency.set_ylabel('Latency (ms)')

tidb.set_zorder(tidb_latency.zorder+1)
tidb.set_frame_on(False)
lgd = fig.legend(handles=[line3[0], error3, box3],ncol=3,bbox_to_anchor=(.78,1.15))

taas3.spines['top'].set_visible(False)
taas5.spines['top'].set_visible(False)
taas7.spines['top'].set_visible(False)
taas9.spines['top'].set_visible(False)
tidb.spines['top'].set_visible(False)
taas3_latency.spines['top'].set_visible(False)
taas5_latency.spines['top'].set_visible(False)
taas7_latency.spines['top'].set_visible(False)
taas9_latency.spines['top'].set_visible(False)
tidb_latency.spines['top'].set_visible(False)
# ax.spines['right'].set_visible(False)
# ax.spines['left'].set_linewidth(2)
# ax.spines['bottom'].set_linewidth(2)

plt.rcParams['font.family'] = 'Helvetica'
xlabel = fig.supxlabel('Parallel Clients per Machine')
fig.savefig('parallel.pdf', format='pdf',bbox_extra_artists=(lgd,xlabel), bbox_inches='tight')