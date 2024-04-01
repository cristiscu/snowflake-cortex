# TODO: as notebook!
import matplotlib.pyplot as plt
import numpy as np

def plot_series(time, series, format="-", start=0, end=None, label=None):
    plt.figure(figsize=(10, 6))
    plt.plot(time[start:end], series[start:end], format)
    plt.xlabel("Time")
    plt.ylabel("Value")
    if label: plt.legend(fontsize=14, labels=label)
    plt.grid(True)
    plt.show()
    
def trend(time, slope=0):
    series = slope * time
    return series

def seasonal_pattern(season_time):
    data_pattern = np.where(season_time < 0.4,
        np.cos(season_time * 2 * np.pi),
        1 / np.exp(3 * season_time))
    return data_pattern

def seasonality(time, period, amplitude=1, phase=0):
    season_time = ((time + phase) % period) / period
    data_pattern = amplitude * seasonal_pattern(season_time)
    return data_pattern

def noise(time, noise_level=1, seed=None):
    rnd = np.random.RandomState(seed)
    noise = rnd.randn(len(time)) * noise_level
    return noise

def autocorrelation(time, amplitude, seed=None):
    rnd = np.random.RandomState(seed)
    ar = rnd.randn(len(time) + 1)
    phi = 0.8
    for step in range(1, len(time) + 1):
        ar[step] += phi * ar[step - 1]
    ar = ar[1:] * amplitude
    return ar

time = np.arange(365)
series = (autocorrelation(time, 10, seed=42)
    + seasonality(time, period=50, amplitude=150)
    + trend(time, 2))
series2 = (autocorrelation(time, 5, seed=42)
    + seasonality(time, period=50, amplitude=2)
    + trend(time, -1)
    + 550)
series[200:] = series2[200:]
plot_series(time[:300], series[:300])
