from pytz import timezone
import numpy as np
from scipy.signal import butter, filtfilt
from datetime import datetime
import glob

from pyjapcscout import PyJapcScout
import datascout as ds


def callback_core(data, h, plotFuncList):
    now_str = datetime.now().strftime('%H:%M:%S')
    out_str = '>> %s %s - '%(h._myPyJapc.getSelector(), now_str)

    if h.saveData:
        # TODO: check if new files appear in the directory
        latest_file = sorted(glob.glob(h.saveDataPath + '/*.parquet'))[-1]
        latest_time_from_name = '.'.join(latest_file.split('/')[-1].split('.')[:-2])
        latest_timestamp = datetime.strptime(latest_time_from_name, '%Y.%m.%d.%H.%M.%S')
        delta_time = datetime.now() - latest_timestamp
        out_str += f'Saving to file {latest_file} ({np.round(delta_time.total_seconds() / 60., 0):.0f} mins ago)'
    else:
        out_str += '*** WARNING: Not saving any data ... ***'
    print(out_str)

    for plotFunc in plotFuncList:
        #df = ds.dict_to_pandas(data).iloc[0]
        #plotFunc.plot(df)
        plotFunc.plot(data)

def getCtime(data,indx=None):
    d = data['value']
    ctime = d['firstSampleTime']
    samplingTrain = d['samplingTrain']
    samples = np.array(d['samples'].tolist())
    n_samples = samples.shape[1]
    n_dim = samples.shape[0]
    ctime += np.arange(n_samples)*samplingTrain
    ctime *= d['timeUnitFactor'] * 1e3 # to ms
    out = np.vstack([ctime]*n_dim)
    if indx==None:
        return out
    else:
        return out[indx]

def getSamples(data,indx=None):
    out = data['value']['samples']
    if indx==None:
        return out
    else:
        return out[indx]

def getSamplesAndCtime(data,indx,cStart=None,cStop=None):
    ctime = getCtime(data,indx)
    if cStart:
        iStart = np.searchsorted(ctime,cStart)
    else:
        iStart = 0
    if cStop:
        iStop = np.searchsorted(ctime,cStop)
    else:
        iStop = len(ctime)
    samples = getSamples(data,indx)#,range(iStart,iStop)))
    ctime = getCtime(data,indx)#,range(iStart,iStop)))
    return samples[iStart:iStop], ctime[iStart:iStop]

def getCycleStamp(data):
    return datetime.utcfromtimestamp(data['header']['cycleStamp']/1e9)

def _getCycleStampLocalTz(cycleStamp):
    return datetime.fromtimestamp(cycleStamp/1e9)

def getCycleStampLocalTz(data):
    return datetime.fromtimestamp(data['header']['cycleStamp']/1e9)

def getSelector(data):
    return data['header']['selector']

def getXY(data,indx=None):
    if indx==None:
        d = data['value']
    else:
        d = data['value'][indx]
    return d['X'],d['Y'] 

def getValueAndTime(data,indx=None,cStart=None,cStop=None):
    time, value = getXY(data,indx)
    if cStart:
        iStart = np.searchsorted(time,cStart)
    else:
        iStart = 0
    if cStop:
        iStop = np.searchsorted(time,cStop)
    else:
        iStop = len(time)
    return value[iStart:iStop], time[iStart:iStop]

def butter_lowpass_filter(data,f_s,cutoff,order=2):
    # f_s ... sample rate, Hz
    # cutoff ... desired cutoff frequency of the filter, Hz
    nyq = 0.5 * f_s
    normal_cutoff = cutoff / nyq
    b, a = butter(order, normal_cutoff, btype='low', analog=False)
    y = filtfilt(b, a, data)
    return y
