
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter

plt.rc('font', size=16)
plt.margins(0)
# throughput_max = 120000
# throughput_tick_num = 3
# throughput_ticks = [throughput_max/throughput_tick_num * i for i in range(throughput_tick_num+1)]
# throughput_labels = [str(int(t/1000))+"k" for t in throughput_ticks]
throughput_max = 100e3
throughput_ticks = [0, 90e3]
throughput_labels = ['0',  '90k']

time_range = 420
time_ticks = [60 * i for i in range(0,int(time_range/60) + 1)]
time_labels = ["%s'00\"" % i for i in range(0,int(time_range/60) + 1)]

def ParsethroughputData(filePath):
# Assuming the log data is stored in a file called log.txt
# Open the file and read the lines
# with open("throughput-line-3az.txt", "r") as f:
    with open(filePath, "r") as f:
        lines = f.readlines()
        data, res = {}, {}
        # Loop through the lines
        for line in lines:
            parts = line.split()
            if len(parts) == 0:
                continue
            if parts[0].startswith("TS"):
                ts = parts[0][3:]
                # Get the throughput value
                if "MISS" in line or "NAN" in line:
                    rps = 0
                else:
                    rps = float(parts[3].split(":")[1].split(",")[0])
                if rps > 30000 * 10:
                    continue
                # Add the throughput value to the dictionary with the timestamp as the key
                data[ts] = data.get(ts, 0) + rps

    # Sort the dictionary by the keys (timestamps)
    data = dict(sorted(data.items()))
    # Initialize a variable to store the starting timestamp
    start_ts = None
    time_skew = 0 # filter warm-up time
    # Loop through the dictionary items
    for ts, rps in data.items():
        # If start_ts is None, set it to the current timestamp
        if start_ts is None:
            if int(rps) > 80000:
                start_ts = int(ts)+time_skew
            else:
                time_skew += 1
                print (ts, rps)
                continue
        # Calculate the elapsed time from the start_ts
        elapsed = int(ts) - int(start_ts)
        # Print the elapsed time and the aggregated throughput value
        if elapsed >= 0:
            res[elapsed] = rps
    return res


if __name__ == "__main__":
    tso_data = ParsethroughputData("throughput-line-3az-tso.log")
    taas_data = ParsethroughputData("throughput-line-3az-taas.log")
    
    tso_time, tso_rps = list(tso_data.keys()), list(tso_data.values())
    taas_time, taas_rps = list(taas_data.keys()), list(taas_data.values())
    fig, axs = plt.subplots(2, 1, figsize=(9, 4))
    plt.subplots_adjust(hspace=0.8, left=0.12, right=0.92, bottom=0.18, top=1.0)
    plt.rcParams['font.family'] = 'Helvetica'

    for i in range(len(axs)):
        # axs[i].set_xlabel('Time')
        axs[i].spines['top'].set_visible(False)
        axs[i].spines['right'].set_visible(False)
        axs[i].spines['left'].set_linewidth(2)
        axs[i].spines['bottom'].set_linewidth(2)
        axs[i].tick_params(axis='both', which='major', width=2)

        #axs[i].spines['left'].set_visible(False)
        axs[i].set_yticks(throughput_ticks)
        axs[i].set_yticklabels(throughput_labels)
        axs[i].set_ylim(0, throughput_max)
        axs[i].set_xlim(0, time_range)
        if i == len(axs)-1:
            axs[i].set_xticks(time_ticks)
            axs[i].set_xticklabels(time_labels)
            axs[i].tick_params(axis='x', which='both', pad=10) 
            # axs[i].xaxis.set_label_coords(-0.01, 0.5)
            axs[i].set_xlabel("Time")
            axs[i].set_ylabel('Throughput')
            axs[i].yaxis.set_label_coords(-0.08, 1.2)
        else:
            axs[i].set_xticks(time_ticks)
            axs[i].set_xticklabels("")
    axs[0].plot(taas_time, taas_rps, c='xkcd:violet', linewidth=2)
    axs[0].set_title('TaaS', loc='left')
    axs[1].plot(tso_time, tso_rps, c='xkcd:rust', label="throughput", linewidth=2)
    axs[1].set_title('TiDB-PD', loc='left')

    # fig.legend(ncol=4, loc='upper center')
    fig.subplots_adjust(top=.92)
    fig.savefig('throughput-line-3az.pdf', format='pdf')