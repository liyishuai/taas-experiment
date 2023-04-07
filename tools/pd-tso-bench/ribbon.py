import matplotlib.pyplot as plt
import numpy as np
import csv
from matplotlib.ticker import ScalarFormatter

#plt.rc('font', size=8)
plt.margins(0)

latency_min = 0.08
latency_max = 100
latency_ticks = [0.1, 0.4, 1, 4, 10, 40, 100]
latency_labels = ['0.1', '0.4', '1', '4', '10', '40', '100']
throughput_max = 90000
throughput_ticks = [0, 2e4, 4e4, 6e4, 8e4]
throughput_labels=['0', '20k', '40k', '60k', '80k']

time_ticks = [0, 60, 120, 180, 240, 300]
time_labels = ["0'00\"", "1'00\"", "2'00\"", "3'00\"", "4'00\"", "5'00\""]

x = np.arange(0, 300, 1)

tso_total = []
tso0 = []
tso50 = []
tso99 = []
tso100 = []

with open('tso.csv', encoding='utf-8-sig') as tso_file:
    taas_read = csv.reader(tso_file, delimiter=',')
    for row in taas_read:
        tso_total.append(float(row[0]))
        tso0.append(float(row[1]))
        tso50.append(float(row[2]))
        tso99.append(float(row[3]))
        tso100.append(float(row[4]))

fig, (taas_latency, tso_latency) = plt.subplots(2, 1, sharex=True, figsize=(8, 4))
plt.subplots_adjust(hspace=0.4)

taas_latency.set_title('TaaS', loc='left')
tso_latency.set_title('TiDB-PD', loc='left')

tso_throughput = tso_latency.twinx()

tso_latency.fill_between(x, tso0, tso50, color='green')
tso_latency.fill_between(x, tso50, tso99, color='orange')
tso_latency.fill_between(x, tso99, tso100, color='aqua')
tso_latency.set_yscale('log')
tso_latency.set_ylim(latency_min, latency_max)
tso_latency.set_yticks(latency_ticks)
tso_latency.set_yticklabels(latency_labels)

tso_throughput.plot(x, tso_total, color='black', label='throughput')
tso_throughput.set_ylim(0, throughput_max)
tso_throughput.set_xlim(0, 300)
tso_throughput.set_yticks(throughput_ticks)
tso_throughput.set_yticklabels(throughput_labels)

taas_total = []
taas0 = []
taas50 = []
taas99 = []
taas100 = []

with open('taas.csv', encoding='utf-8-sig') as taas_file:
    taas_read = csv.reader(taas_file, delimiter=',')
    for row in taas_read:
        taas_total.append(float(row[0]))
        taas0.append(float(row[1]))
        taas50.append(float(row[2]))
        taas99.append(float(row[3]))
        taas100.append(float(row[4]))

taas_throughput = taas_latency.twinx()
taas_throughput.set_ylim(0, throughput_max)
taas_throughput.set_xlim(0, 300)

taas_latency.fill_between(x, taas99, taas100, color='aqua', label='latency 99~100%')
taas_latency.fill_between(x, taas50, taas99, color='orange', label='latency 50~99%')
taas_latency.fill_between(x, taas0, taas50, color='green', label='latency 00~50%')
taas_latency.set_yscale('log')
taas_latency.set_ylim(latency_min, latency_max)
taas_latency.set_yticks(latency_ticks)
taas_latency.set_yticklabels(latency_labels)

taas_throughput.plot(x, taas_total, color='black')
taas_throughput.set_xticks(time_ticks)
taas_throughput.set_xticklabels(time_labels)
taas_throughput.set_yticks(throughput_ticks)
taas_throughput.set_yticklabels(throughput_labels)
taas_throughput.set_xlabel('Time')

fig.subplots_adjust(top=.8)#, right=.9)
fig.legend(ncol=4, loc='upper center')
tso_latency.set_xlabel('Execution Time')
tso_latency.set_ylabel('Latency (ms)')
taas_latency.set_ylabel('Latency (ms)')
tso_throughput.set_ylabel('Throughput')
taas_throughput.set_ylabel('Throughput')

fig.savefig('result.pdf', format='pdf')