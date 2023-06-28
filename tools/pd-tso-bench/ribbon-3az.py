import matplotlib.pyplot as plt
import numpy as np
import csv
from matplotlib.ticker import ScalarFormatter

#plt.rc('font', size=8)
time_range = 180
time_inteval = 60
latency_min = 0.1
latency_max = 10
latency_ticks = [0.1, 0.4, 1, 4, 10]
latency_labels = ['0.1', '0.4', '1', '4', '10']
throughput_max = 80000
throughput_ticks = [0, 2e4, 4e4, 6e4, 8e4]
throughput_labels=['0', '20k', '40k', '60k', '80k']
time_ticks = [0, 60, 120, 180]
time_labels = ["0'00\"", "1'00\"", "2'00\"", "3'00\""]
x = np.arange(0, time_range, 1)

def ParseLatencyCSV(filePath):
    tso_total = []
    tso0 = []
    tso50 = []
    tso99 = []
    tso100 = []
    with open(filePath, encoding='utf-8-sig') as tso_file:
        data_read = csv.reader(tso_file, delimiter=',')
        for row in data_read:
            for i in range(len(row)):
                if "NAN" in str(row[i]) or "MISS" in str(row[i]) :
                    row[0],row[1],row[2] = 0,0,0  
                    break
            tso_total.append(float(row[0]))
            tso50.append(float(row[1]))
            tso99.append(float(row[2]))       
    return tso_total, tso50, tso99

def PlotByAzAndSysType(az=""):

    SYS_TYPE_PLT = {"taas": None, "tso": None}
    fig, (SYS_TYPE_PLT["taas"], SYS_TYPE_PLT["tso"]) = plt.subplots(2, 1, sharex=True, figsize=(8, 4))
    # set figure
    plt.margins(0)
    plt.subplots_adjust(hspace=0.3)
    for sysType in SYS_TYPE_PLT.keys():
        print ("Generating %s-%s" % (sysType, az) )
        # get stat 
        filePath = sysType + "-" + az + ".csv" if len(az) else sysType + ".csv"
        tso_throughput = SYS_TYPE_PLT[sysType]
        tso_total, tso50, tso99 = ParseLatencyCSV(filePath)  #filePath
        #FIXME:zgh fill blank
        tso_total.extend([0.0 for i in range(time_range-len(tso_total))])
        tso50.extend([0.0 for i in range(time_range-len(tso50))])
        tso99.extend([0.0 for i in range(time_range-len(tso99))])
        #
        tso_latency = tso_throughput.twinx()
        sysName = "TaaS" if sysType in "taas" else "TiDB-PD"
        tso_latency.set_title(sysName+"-"+str(az).upper(), loc='left')
        tso_latency.fill_between(x, [0 for i in range(time_range)], tso50, color='green')
        tso_latency.fill_between(x, tso50, tso99, color='orange')
        tso_latency.set_yscale('log')
        tso_latency.set_ylim(latency_min, latency_max)
        tso_latency.set_yticks(latency_ticks)
        tso_latency.set_yticklabels(latency_labels)
        tso_latency.set_xlabel('Execution Time')
        tso_latency.set_ylabel('Latency (ms)')
        #
        tso_throughput.plot(x, tso_total, color='black', label='throughput')
        tso_throughput.set_ylim(0, throughput_max)
        tso_throughput.set_xlim(0, time_range)
        tso_throughput.set_yticks(throughput_ticks)
        tso_throughput.set_yticklabels(throughput_labels)
        tso_throughput.set_xticks(time_ticks)
        tso_throughput.set_xticklabels(time_labels)
        tso_throughput.set_xlabel('Time')
        tso_throughput.set_ylabel('Throughput')
        tso_throughput.set_frame_on(False)
        tso_throughput.set_zorder(tso_latency.zorder+1)
    fig.subplots_adjust(top=.85)
    fig.legend(ncol=4, loc='upper center')
    fig.savefig('result-' + az + '.pdf', format='pdf')

if __name__ == "__main__":
    for az in ["nt99", "nu112", "nm125"]: 
        PlotByAzAndSysType(az) 