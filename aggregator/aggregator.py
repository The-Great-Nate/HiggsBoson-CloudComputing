import awkward as ak
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator
import json
import os
import pika
import time


while True:
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters('rabbitmq')
        )
        break
    except Exception as e:
        print("RabbitMQ not ready, retrying in 2s...", e)
        time.sleep(2)
        
channel = connection.channel()
channel.queue_declare(queue='aggregate v3', durable=True)
expected = None
processed = 0
samples = {}

def block_plotting(channel, method, properties, body):
    global expected, processed, samples # Yeah global variables are bad practice but these two variables are only needed here
    msg = json.loads(body)

    if msg.get('file_count'):
        expected = int(msg['file_count'])
        print(f"Aggregator: updated expected file count to {expected}")
        
    if msg.get('metadata'):
        samples = msg['metadata']
        print(f"Aggregator: received sample metadata: {samples}")

    if msg.get("file_done"):
        processed += 1
        print(f"Processed {processed}/{expected}")

        if expected is not None and processed == expected:
            print("All files processed! Proceeding to plotting...")
            channel.stop_consuming()
            
    channel.basic_ack(method.delivery_tag)
        

channel.basic_consume(queue='aggregate v3', on_message_callback=block_plotting, auto_ack=False)

print("Waiting for all files to finish processing before plotting...")
channel.start_consuming()
connection.close()


GeV = 1.0
lumi = 36.6
fraction = 1.0

data_dir = "/data/"
figures_dir = "/data/figures"
os.makedirs(figures_dir, exist_ok=True)

# x-axis range of the plot
xmin = 80 * GeV
xmax = 250 * GeV
# Histogram bin setup
step_size = 2.5 * GeV
bin_edges = np.arange(start=xmin, # The interval includes this value
                    stop=xmax+step_size, # The interval doesn't include this value
                    step=step_size ) # Spacing between values
bin_centres = np.arange(start=xmin+step_size/2, # The interval includes this value
                        stop=xmax+step_size/2, # The interval doesn't include this value
                        step=step_size ) # Spacing between values
    
all_samples_from_workers = {}

for frame_file in os.listdir(data_dir):
    if frame_file.endswith(".parquet"):
        path = os.path.join(data_dir, frame_file)
        print(frame_file)
        sample_name = frame_file.split("-")[0]
        print(frame_file, "->", sample_name)
        frames = ak.from_parquet(path)  # read Awkward Array
        print(f"Loaded {frame_file}, {len(frames)} events")
        
        if sample_name not in all_samples_from_workers:
            all_samples_from_workers[sample_name] = []

        all_samples_from_workers[sample_name].append(frames)

all_data = {}

for sample_name, frame_list in all_samples_from_workers.items():
    print(f"Concatenating {sample_name}: {len(frame_list)} chunks")
    all_data[sample_name] = ak.concatenate(frame_list)
#print(f"Total events after concatenation: {len(all_data)}")

print(f"keys from all_data: {all_data.keys()}")
print(f"all data Data {all_data['Data']}")
data_x,_ = np.histogram(ak.to_numpy(all_data['Data']['mass']),
                        bins=bin_edges ) # histogram the data
data_x_errors = np.sqrt( data_x ) # statistical error on the data

signal_x = ak.to_numpy(all_data[r'Signal ($m_H$ = 125 GeV)']['mass']) # histogram the signal
signal_weights = ak.to_numpy(all_data[r'Signal ($m_H$ = 125 GeV)'].totalWeight) # get the weights of the signal events
signal_color = samples[r'Signal ($m_H$ = 125 GeV)']['color'] # get the colour for the signal bar

mc_x = [] # define list to hold the Monte Carlo histogram entries
mc_weights = [] # define list to hold the Monte Carlo weights
mc_colors = [] # define list to hold the colors of the Monte Carlo bars
mc_labels = [] # define list to hold the legend labels of the Monte Carlo bars

for s in samples: # loop over samples
    if s not in ['Data', r'Signal ($m_H$ = 125 GeV)']: # if not data nor signal
        mc_x.append( ak.to_numpy(all_data[s]['mass']) ) # append to the list of Monte Carlo histogram entries
        mc_weights.append( ak.to_numpy(all_data[s].totalWeight) ) # append to the list of Monte Carlo weights
        mc_colors.append( samples[s]['color'] ) # append to the list of Monte Carlo bar colors
        mc_labels.append( s ) # append to the list of Monte Carlo legend labels

# *************
# Main plot
# *************
fig, main_axes = plt.subplots(figsize=(12, 8))

# plot the data points
main_axes.errorbar(x=bin_centres, y=data_x, yerr=data_x_errors,
                    fmt='ko', # 'k' means black and 'o' is for circles
                    label='Data')

# plot the Monte Carlo bars
mc_heights = main_axes.hist(mc_x, bins=bin_edges,
                            weights=mc_weights, stacked=True,
                            color=mc_colors, label=mc_labels )

mc_x_tot = mc_heights[0][-1] # stacked background MC y-axis value

# calculate MC statistical uncertainty: sqrt(sum w^2)
mc_x_err = np.sqrt(np.histogram(np.hstack(mc_x), bins=bin_edges, weights=np.hstack(mc_weights)**2)[0])

# plot the signal bar
signal_heights = main_axes.hist(signal_x, bins=bin_edges, bottom=mc_x_tot,
                weights=signal_weights, color=signal_color,
                label=r'Signal ($m_H$ = 125 GeV)')

# plot the statistical uncertainty
main_axes.bar(bin_centres, # x
                2*mc_x_err, # heights
                alpha=0.5, # half transparency
                bottom=mc_x_tot-mc_x_err, color='none',
                hatch="////", width=step_size, label='Stat. Unc.' )

# set the x-limit of the main axes
main_axes.set_xlim( left=xmin, right=xmax )

# separation of x axis minor ticks
main_axes.xaxis.set_minor_locator( AutoMinorLocator() )

# set the axis tick parameters for the main axes
main_axes.tick_params(which='both', # ticks on both x and y axes
                        direction='in', # Put ticks inside and outside the axes
                        top=True, # draw ticks on the top axis
                        right=True ) # draw ticks on right axis

# x-axis label
main_axes.set_xlabel(r'4-lepton invariant mass $\mathrm{m_{4l}}$ [GeV]',
                    fontsize=13, x=1, horizontalalignment='right' )

# write y-axis label for main axes
main_axes.set_ylabel('Events / '+str(step_size)+' GeV',
                        y=1, horizontalalignment='right')

# set y-axis limits for main axes
main_axes.set_ylim( bottom=0, top=np.amax(data_x)*2.0 )

# add minor ticks on y-axis for main axes
main_axes.yaxis.set_minor_locator( AutoMinorLocator() )

# Add text 'ATLAS Open Data' on plot
plt.text(0.1, # x
            0.93, # y
            'ATLAS Open Data', # text
            transform=main_axes.transAxes, # coordinate system used is that of main_axes
            fontsize=16 )

# Add text 'for education' on plot
plt.text(0.1, # x
            0.88, # y
            'for education', # text
            transform=main_axes.transAxes, # coordinate system used is that of main_axes
            style='italic',
            fontsize=12 )

# Add energy and luminosity
lumi_used = str(lumi*fraction) # luminosity to write on the plot
plt.text(0.1, # x
            0.82, # y
            r'$\sqrt{s}$=13 TeV,$\int$L dt = '+lumi_used+' fb$^{-1}$', # text
            transform=main_axes.transAxes,fontsize=16 ) # coordinate system used is that of main_axes

# Add a label for the analysis carried out
plt.text(0.1, # x
            0.76, # y
            r'$H \rightarrow ZZ^* \rightarrow 4\ell$', # text
            transform=main_axes.transAxes,fontsize=16 ) # coordinate system used is that of main_axes

# draw the legend
my_legend = main_axes.legend( frameon=False, fontsize=16 ) # no box around the legend

fig_path = os.path.join(figures_dir, "final_histogram.pdf")
plt.savefig(fig_path, bbox_inches='tight')
print(f"Figure saved to {fig_path}") 