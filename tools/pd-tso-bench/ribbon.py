#!/usr/bin/env python3
import matplotlib.pyplot as plt
import numpy as np
import csv
from matplotlib.ticker import ScalarFormatter

#plt.rc('font', size=8)
plt.margins(0)

latency_min = 0.1
latency_max = 2
latency_ticks = [0.1, 0.4, 1, 2]
latency_labels = ['0.1', '0.4', '1', '2']
throughput_max = 4e4
throughput_ticks = [0, 1e4, 2e4, 3e4, 4e4]
throughput_labels=['0', '10k', '20k', '30k', '40k']

time_ticks = [0, 60, 120, 180, 240, 300]
time_labels = ["0'00\"", "1'00\"", "2'00\"", "3'00\"", "4'00\"", "5'00\""]

tso_total = []
tso0 = []
tso50 = []
tso99 = []
tso100 = []

with open('tso.csv', encoding='utf-8-sig') as tso_file:
    tso_read = csv.reader(tso_file, delimiter=',')
    for row in tso_read:
        for i in range(len(row)):
            if "NAN" in str(row[i]) or "MISS" in str(row[i]) :
                row[0],row[1],row[2] = 0,np.NaN,np.NaN  
                break
        tso_total.append(float(row[0]))
        tso50.append(float(row[1]))
        tso99.append(float(row[2]))

fig, (taas_throughput, tso_throughput) = plt.subplots(2, 1, sharex=True, figsize=(9, 4))
plt.subplots_adjust(hspace=0.3, left=0.08, right=0.92, bottom=0.12, top=1.0)

tso_latency = tso_throughput.twinx()
taas_latency = taas_throughput.twinx()

taas_latency.set_title('TaaS', loc='left')
tso_latency.set_title('TiDB-PD', loc='left')

tso_latency.fill_between(np.arange(0, len(tso50), 1), [0 for i in range(len(tso50))], tso50, color='green')
tso_latency.fill_between(np.arange(0, len(tso99), 1), tso50, tso99, color='orange')
tso_latency.set_yscale('log')
tso_latency.set_ylim(latency_min, latency_max)
tso_latency.set_yticks([0.1, 0.2, 0.3, 1, 2])
tso_latency.set_yticklabels(['0.1', '0.2', '0.3', '1', '2'])

tso_throughput.plot(np.arange(0, len(tso_total), 1), tso_total, color='black', label='throughput')
tso_throughput.set_ylim(0, throughput_max)
tso_throughput.set_xlim(0, 300)
tso_throughput.set_yticks(throughput_ticks)
tso_throughput.set_yticklabels(throughput_labels)
tso_throughput.set_xticks(time_ticks)
tso_throughput.set_xticklabels(time_labels)
tso_throughput.set_xlabel('Time')

taas_total = []
taas0 = []
taas50 = []
taas99 = []
taas100 = []

with open('taas.csv', encoding='utf-8-sig') as taas_file:
    tso_read = csv.reader(taas_file, delimiter=',')
    for row in tso_read:
        for i in range(len(row)):
            if "NAN" in str(row[i]) or "MISS" in str(row[i]) :
                row[0],row[1],row[2] = 0,0,0  
                break
        taas_total.append(float(row[0]))
        taas50.append(float(row[1]))
        taas99.append(float(row[2]))

taas_throughput.set_ylim(0, throughput_max)
taas_throughput.set_xlim(0, 300)

taas_latency.fill_between(np.arange(0, len(taas50), 1), [0 for i in range(len(taas50))], taas50, color='green', label='50% latency')
taas_latency.fill_between(np.arange(0, len(taas99), 1), taas50, taas99, color='orange', label='99% latency')
taas_latency.set_yscale('log')
taas_latency.set_ylim(latency_min, latency_max)
taas_latency.set_yticks([0.1, 0.2, 0.4, 0.7, 1, 2])
taas_latency.set_yticklabels(['0.1', '0.2', '0.4', '0.7', '1', '2'])

taas_throughput.plot(np.arange(0, len(taas_total), 1), taas_total, color='black')
taas_throughput.set_yticks(throughput_ticks)
taas_throughput.set_yticklabels(throughput_labels)

fig.subplots_adjust(top=.85)
fig.legend(ncol=4, loc='upper center', bbox_to_anchor=(0.5, 1.01))
tso_latency.set_xlabel('Execution Time')
tso_latency.set_ylabel('Latency (ms)')
taas_latency.set_ylabel('Latency (ms)')
tso_throughput.set_ylabel('Throughput')
taas_throughput.set_ylabel('Throughput')

tso_throughput.set_frame_on(False)
taas_throughput.set_frame_on(False)
tso_throughput.set_zorder(tso_latency.zorder+1)
taas_throughput.set_zorder(taas_latency.zorder+1)
fig.subplots_adjust(top=.92)
fig.savefig('result.pdf', format='pdf')