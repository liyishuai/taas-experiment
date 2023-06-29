
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter

plt.margins(0)
throughput_max = 50000
throughput_tick_num = 5
throughput_ticks = [throughput_max/throughput_tick_num * i for i in range(throughput_tick_num+1)]
throughput_labels = [str(int(t/1000))+"k" for t in throughput_ticks]
time_ticks = [0, 60, 120, 180]
time_labels = ["0'00\"", "1'00\"", "2'00\"", "3'00\""]
time_range = 180

def ParseThrouputData(filePath):
# Assuming the log data is stored in a file called log.txt
# Open the file and read the lines
# with open("throuput-line-3az.txt", "r") as f:
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
    time_skew = 0
    # Loop through the dictionary items
    for ts, rps in data.items():
        # If start_ts is None, set it to the current timestamp
        if start_ts is None:
            if int(rps) > 25000:
                start_ts = int(ts)
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
    tso_data = ParseThrouputData("throuput-line-3az-tso.log")
    taas_data = ParseThrouputData("throuput-line-3az-taas.log")
    
    tso_time, tso_rps = list(tso_data.keys()), list(tso_data.values())
    taas_time, taas_rps = list(taas_data.keys()), list(taas_data.values())
    plt.subplots_adjust(hspace=0.3)
    fig, axs = plt.subplots(2, 1, figsize=(10, 5))

    for i in range(len(axs)):
        # axs[i].set_xlabel('Time')
        axs[i].set_ylabel('Throuput')
        # axs[i].set_frame_on(False)
        axs[i].set_yticks(throughput_ticks)
        axs[i].set_yticklabels(throughput_labels)
        axs[i].set_xticks(time_ticks)
        axs[i].set_xticklabels(time_labels)
        axs[i].set_ylim(0, throughput_max)
        axs[i].set_xlim(0, time_range)
    axs[0].plot(tso_time, tso_rps)
    axs[0].set_title('TIDB-PD')
    axs[1].plot(taas_time, taas_rps)
    axs[1].set_title('TaaS')

    # fig.legend(ncol=4, loc='upper center')
    fig.subplots_adjust(top=.85)
    fig.savefig('throuput-line-3az.pdf', format='pdf')