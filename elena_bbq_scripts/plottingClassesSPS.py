import matplotlib.gridspec as gridspec
import numpy as np
import matplotlib.dates as mdates
import matplotlib
from datetime import datetime
import matplotlib.pyplot as plt
from pytz import timezone
from matplotlib import cm
from scipy.signal import butter,filtfilt
from scipy.signal import savgol_filter
from matplotlib import cm
from mpl_toolkits.axes_grid1 import make_axes_locatable
import time
import pandas as pd
import os

import helper_functions as hf
import WireScannerAnalysis as wsa


colors = [c[1] for c in enumerate(plt.rcParams['axes.prop_cycle'].by_key()['color'][:10])]

def truncate_colormap(cmap, minval=0.0, maxval=1.0, n=100):
    new_cmap = matplotlib.colors.LinearSegmentedColormap.from_list(
        'trunc({n},{a:.2f},{b:.2f})'.format(n=cmap.name, a=minval, b=maxval),
        cmap(np.linspace(minval, maxval, n)))
    return new_cmap

class PlottingClassSPS():

    def __init__(self,figsize,pngfolder=None,*args,**kw):
        self.pngfolder = pngfolder
        self.figsize = figsize
        self.figname = self.__class__.__name__
        self._nrows = 1
        self._ncols = 1
        self._sharex = False
        self._sharey = False

    def createFigure(self):
        self.figure = plt.figure(self.figname,figsize=self.figsize)
        plt.show(block=False)

    def initializeFigure(self):
        self.createFigure()
        self.drawFigure()

    def clearFigure(self):
        self.figure.clear()

    def removeLines(self):
        axs = self._getAxes()
        for ax in axs:
            for line in ax.get_lines():
                line.remove()

    def clearAxes(self):
        axs = self._getAxes()
        [ax.cla() for ax in axs]

    def createSubplots(self,*args,**kwargs):
        nrows = self._nrows
        ncols = self._ncols
        num = self.figname
        sharex = self._sharex
        sharey = self._sharey
        f,axs = plt.subplots(nrows,ncols,num=num,
                    sharex=sharex,sharey=sharey,*args,**kwargs)
        self.axs = axs
        return f, axs

    def _getAxes(self):
        axs = self.figure.get_axes()
        axs = [axs] if not isinstance(axs, list) else axs
        return axs

    def setFigureSize(self):
        self.figure.set_size_inches(*self.figsize) 

    def drawFigure(self):
        self.figure.canvas.draw()
        self.figure.canvas.flush_events()

    def generateTitleStr(self,data):
        ts_str = hf.getCycleStampLocalTz(data).strftime('(%d.%m.%Y - %H:%M:%S)')
        user = hf.getSelector(data)
        return user+' '+ts_str

    def saveFigure(self,filename):
        pass


class BCT(PlottingClassSPS):

    def __init__(self,devices,figsize=(6,4),*args,**kw):
        super().__init__(figsize,*args,**kw)
        self.initializeFigure()
        self.devices = devices

    def initializeFigure(self):
        super().initializeFigure()
        plt.subplots_adjust(top=0.914,bottom=0.146,left=0.15,right=0.975)

    def plot(self,data):
        if not plt.fignum_exists(self.figname):
            return       
        self.clearFigure()
        figure, ax = self.createSubplots()
        for ii,device in enumerate(self.devices):
            d = data[device]['value']
            bct = d['totalIntensity']*np.power(10,d['totalIntensity_unitExponent'])
            ctime = d['measStamp']*np.power(10,d['measStamp_unitExponent'])
            ax.plot(ctime,bct,label=device,c=colors[ii])
            acqTime = d['acqTime']
        ax.set_title(self.generateTitleStr(data[device]),fontsize=10)
        ax.set_xlabel('time (s)')
        ax.set_ylabel('total intensity (p)')
        ax.grid('on')
        ax.autoscale(enable=True, axis='y')
        ax.legend()
        self.drawFigure()
        

class Pressure(PlottingClassSPS):
    def __init__(self,devices=[],figsize=(6,4),*args,**kw):
        super().__init__(figsize,*args,**kw)
        self.devices = devices
        self.initializeFigure()
    def initializeFigure(self):
        self.createFigure()
        self.clearFigure()
        ax = plt.axes()
        plt.subplots_adjust(top=0.914,bottom=0.146,left=0.15,right=0.975)
        #ax.set_xlabel('time (s)')
        ax.set_ylabel('Pressure (mbar)')
        if not ax.get_legend_handles_labels()[0]:
            for device in self.devices:
                ax.plot(datetime.utcnow(),np.nan,'r.',label=device)#,c=colors[ring])
        ax.plot(datetime.utcnow(),0,'-')
        ax.autoscale(enable=True, axis='x')
        ax.autoscale(enable=True, axis='y')
        ax.legend(ncol=1)

        ax.grid('on')
        ax.autoscale(enable=True, axis='y')
        self.drawFigure()
        self.ax = ax
    def plot(self,data):
        figure,ax = self.figure,self.ax
        if not plt.fignum_exists(self.figname):
            return       
        for device in self.devices:
            ts = data[device]['header']['acqStamp']
            user = hf.getSelector(data[device])
            ax.plot(ts,data[device]['value'],'r.')
#        ax.set_title(user+' '+ts_str,fontsize=10)
        ax.relim()
        ax.autoscale(enable=True, axis='x')
        figure.autofmt_xdate() 
        myFmt = mdates.DateFormatter('%d.%m %H:%M')#'%d %b %Y %H:%M:%S'
        myFmt.set_tzinfo(timezone('CET'));
        ax.get_xaxis().set_major_formatter(myFmt)
        self.drawFigure()
                

class FBCT(PlottingClassSPS):

    def __init__(self,device,stride=5,figsize=(8, 7),*args,**kw):
        super().__init__(figsize,*args,**kw)
        self._nrows = 2
        self._sharex = True
        self.initializeFigure()
        self._isFirstAcquisition = True
        self.device = device
        self.stride = stride
        self.cmap = 'Spectral'
        

    def plot(self, data):
        self.clearFigure()
        figure, axs = self.createSubplots()
        d = data[self.device]['value']
        fillingPattern = d['fillingPattern']
        last_measStamp_with_beam = max(sum(fillingPattern))
        filledSlots = np.where(np.sum(fillingPattern,axis=0))[0]

        unit = np.power(10, d['bunchIntensity_unitExponent'])
        n_meas_in_cycle, n_slots = d['bunchIntensity'].shape
        im = axs[0].pcolormesh(range(n_slots), 1e-3*d['measStamp'], 
                d['bunchIntensity'] * unit, cmap=self.cmap,shading='nearest')
        cb1 = plt.colorbar(im,ax=axs[0])
        cb1.set_label('Intensity')
        cmap = matplotlib.cm.get_cmap(self.cmap)
        
        indcs = np.arange(0,d['nbOfMeas'],self.stride)
        for i,indx in enumerate(indcs):
            bunch_intensities = d['bunchIntensity'][indx, :] * unit
            c = cmap(float(i)/len(indcs))
            axs[1].plot(bunch_intensities, color=c)
        sm = plt.cm.ScalarMappable(cmap=cmap, 
            norm=plt.Normalize(vmin=min(1e-3*d['measStamp']), vmax=max(1e-3*d['measStamp'])))
        cb2 = plt.colorbar(sm,ax=axs[1])
        cb2.set_label('Cycle time (s)')

        axs[0].set_title(self.generateTitleStr(data[self.device]), fontsize=10)
        if d['beamDetected']:
            axs[1].set_xlim(min(filledSlots)-20,max(filledSlots)+20)
            axs[0].set_ylim(1e-3*d['measStamp'][0], 
                1e-3*d['measStamp'][last_measStamp_with_beam+2])
        self.axs[0].set_ylabel('Cycle time (s)')
        self.axs[1].set_xlabel('25 ns slot')
        self.axs[1].set_ylabel('Intensity')
        plt.tight_layout()
        self.drawFigure()


class FBCTTT10(PlottingClassSPS):

    def __init__(self, devices=['TT10.BCTFI.102834/CaptureAcquisition'], 
            figsize=(6, 4), *args, **kw):
        super().__init__(figsize,*args,**kw)
        self.devices = devices
        self.initializeFigure()
        self.devices = devices

    def plot(self, data):
        if not plt.fignum_exists(self.figname):
            return
        self.clearFigure()
        figure, axs = self.createSubplots()
        for i,device in enumerate(self.devices):
            d = data[device]['value']
            unit = np.power(10, d['bunchIntensity_unitExponent'])
            for j in range(d['nbOfMeas']):
                bunch_intensities = d['bunchIntensity'][j] * unit
                axs.plot(bunch_intensities, color=colors[j],label='pulse %d'%(j+1))
        axs.set_title(self.generateTitleStr(data[device]),fontsize=10)
        axs.legend(loc=1)
        self.axs.set_ylabel('Intensity (p)')
        self.axs.set_xlabel('25 ns slot')
        plt.tight_layout()
        self.drawFigure()


class FBCTTTL(PlottingClassSPS):

    def __init__(self, devices=['TT10.BCTFI.102834/CaptureAcquisition'], 
            figsize=(6.5, 4), *args, **kw):
        #self._nrows = len(devices)
        super().__init__(figsize,*args,**kw)
        self.devices = devices
        self.initializeFigure()
        self.devices = devices

    def plot(self, data):
        if not plt.fignum_exists(self.figname):
            return
        self.clearFigure()
        figure, axs = self.createSubplots()
        for i,device in enumerate(self.devices):
            d = data[device]['value']
            unit = np.power(10, d['bunchIntensity_unitExponent'])
            label = device.split('/')[0]
            for j in range(d['nbOfMeas']):
                if self._nrows > 1:
                    ax = axs[i]
                else:
                    ax = axs
                bunch_intensities = d['bunchIntensity'][j] * unit
                if 'TT10' in device:
                    ax.fill_between(range(len(bunch_intensities)),bunch_intensities,
                       color=colors[j],label='%s pulse %d'%(label, j+1),alpha=0.5)
                    ax.plot(bunch_intensities, color=colors[j])
                else:
                    ax.plot(bunch_intensities, color=colors[j],
                       label='%s pulse %d'%(label, j+1))
            ax.set_title(self.generateTitleStr(data[device]),fontsize=10)
            ax.legend(loc=1)
            ax.set_ylabel('Intensity (p)')
            ax.set_xlabel('25 ns slot')
        plt.tight_layout()
        self.drawFigure()

class BBQQC(PlottingClassSPS):

    def __init__(self, device='SPS.BQ.QC/Acquisition', quadrant=1, figsize=(7, 8), *args, **kw):
        super().__init__(figsize,*args,**kw)
        self._nrows = 2
        self._sharex = True
        self._sharey = True
        self.initializeFigure()
        self.device = device
        self.quadrant = quadrant

    def plot(self, data):
        if not plt.fignum_exists(self.figname):
            return
        self.clearFigure()
        figure, axs = self.createSubplots()
        d = data[self.device]['value']
        n_meas, n_fft = d['fftMagnitudeDataH'].shape
        tune_vect = np.linspace(0., 0.5, n_fft)
        if (self.quadrant == 1) or (self.quadrant == 4):
            tuneH_vect = tune_vect
        elif (self.quadrant == 2) or (self.quadrant == 3):
            tuneH_vect = 1 - tune_vect
        if (self.quadrant == 1) or (self.quadrant == 2):
            tuneV_vect = tune_vect
        elif (self.quadrant == 3) or (self.quadrant == 4):
            tuneV_vect = 1 - tune_vect
        time_vect = d['measStamp']
        cmap = truncate_colormap(plt.get_cmap('jet'),0.25,1)
        axs[0].pcolormesh(time_vect, tuneH_vect, d['fftMagnitudeDataH'].T, 
            shading='nearest',cmap=cmap)
        axs[1].pcolormesh(time_vect, tuneV_vect, d['fftMagnitudeDataV'].T, 
            shading='nearest',cmap=cmap)
        axs[0].set_title(self.generateTitleStr(data[self.device]),fontsize=10)
        axs[1].set_xlabel('Cycle time (ms)')
        axs[0].set_ylabel('Tune H')
        axs[1].set_ylabel('Tune V')
        plt.tight_layout()
        self.drawFigure()


class BBQCONT(PlottingClassSPS):

    def __init__(self, device='SPS.BQ.CONT/ContinuousAcquisition',figsize=(7, 8),*args,**kw):
        super().__init__(figsize,*args,**kw)
        self._nrows = 2
        self._sharex = True
        self._sharey = True
        self.initializeFigure()
        self.device = device

    def plot(self, data):
        if not plt.fignum_exists(self.figname):
            return
        self.clearFigure()
        figure, axs = self.createSubplots()
        d = data[self.device]['value']
        msTags = d['msTags']
        frevFreq = d['frevFreq']
        time_vec = []
        for turns,frev in zip(msTags,frevFreq):
            time_vec.extend([1./frev]*(turns-len(time_vec)))
        time_vec.extend([1./frev]*(len(d['rawDataH'])-len(time_vec)))
        time_vec = np.cumsum(time_vec)
        for ii,plane in enumerate(['H', 'V']):
            raw_data = d['rawData%s'%plane]
            axs[ii].plot(time_vec,raw_data)
            axs[ii].set_ylabel('raw position %s (a.u.)'%plane)
        axs[0].set_title(self.generateTitleStr(data[self.device]),fontsize=10)
        axs[1].set_xlabel('Cycle time (ms)')
        plt.tight_layout()
        self.drawFigure()


class ECLOUD(PlottingClassSPS):

    def __init__(self, devices, mode= '1D', figsize=(10, 10), *args, **kw):
        super().__init__(figsize,*args,**kw)
        self._nrows = 2
        self._ncols = 2
        self._sharex = True
        self._sharey = True
        self._leftmargin = 0.1
        self._rightmargin = 0.02
        self._bottommargin = 0.05
        self._topmargin = 0.05
        self._hspace = 0.11
        self._subhspace = 0.05
        self._wspace = 0.11
        self._width = (1-self._wspace*(self._ncols-1)-\
                    self._leftmargin-self._rightmargin)/self._ncols
        self._height = (1-self._hspace*(self._nrows-1)-\
                    self._bottommargin-self._topmargin)/self._nrows
        self._sptopheightratio = 0.3
        self.initializeFigure()
        self.devices = devices
        self.mode = mode
        self.liner = {}
        self.liner['BESCLD-VECM11733/Acquisition'] = ' (Cu-LESS, cleaned)'
        self.liner['BESCLD-VECM11737/Acquisition'] = ' (SS-MBB, fresh)'
        self.liner['BESCLD-VECM11738/Acquisition'] = ' (CNe13 - since 2008)'
        self.liner['BESCLD-VECM11754/Acquisition'] = ' (Cr2O3 on Al - for M. Barnes)'
        self.deadChannels = {l: [] for l in self.liner.keys()}
        self.deadChannels['BESCLD-VECM11754/Acquisition'].append(34)
        self.deadChannels['BESCLD-VECM11737/Acquisition'].append(14)
        self.signalInversion = {l: 1 for l in self.liner.keys()}
        #self.signalInversion['BESCLD-VECM11754/Acquisition'] = 1
        self._nChannelsMax = 48

    def createSubplots2(self,*args,**kwargs):
        nrows = self._nrows
        ncols = self._ncols
        num = self.figname
        sharex = self._sharex
        sharey = self._sharey
        leftmargin = self._leftmargin
        rightmargin = self._rightmargin
        bottommargin = self._bottommargin
        topmargin = self._topmargin
        hspace= self._hspace
        subhspace = self._subhspace
        wspace = self._wspace
        width = self._width
        height = self._height
        sptopheightratio = self._sptopheightratio
        axes = []
        for i in range(ncols):
            axes.append([])
            for j in np.arange(nrows)[::-1]:
                xmin = leftmargin+i*width+(i)*wspace
                ymin = bottommargin+j*height+(j)*hspace
                topheight = sptopheightratio*(height-subhspace)
                topymin = ymin+subhspace + (height-subhspace)*(1-sptopheightratio)
                sp1=self.figure.add_axes([xmin,topymin,width,topheight])
                bottomheight = (1-sptopheightratio)*(height-subhspace)
                sp2=self.figure.add_axes([xmin,ymin,width,bottomheight])
                axes[i].extend([sp1,sp2])
        self.axs = np.array(axes).T
        if self._sharex:
            for i in range(ncols):
                for j in np.arange(nrows):
                    axes[i][2*j].get_shared_x_axes().join(axes[i][2*j],axes[0][0])
                    axes[i][2*j+1].get_shared_x_axes().join(axes[i][2*j+1],axes[0][1])
        if self._sharey:
            for i in range(ncols):
                for j in np.arange(nrows):
                    axes[i][2*j].get_shared_y_axes().join(axes[i][2*j],axes[0][0])
                    axes[i][2*j+1].get_shared_y_axes().join(axes[i][2*j+1],axes[0][1])
        return self.figure, self.axs

    '''
    def plot(self, data):
        if not plt.fignum_exists(self.figname):
            return
        self.clearFigure()
        figure, axs = self.createSubplots()
        for j, device in enumerate(self.devices):
            ax = axs.flatten()[j]
            d = data[device]['value']
            n_channels, n_meas_2d = d['sem2DRaw'].shape
            n_meas_eff = d['nbOfMeas']
            d['sem2DRaw'] = d['sem2DRaw'].astype(np.float) / d['totalGain']
            d['sem2DRaw'][self.deadChannels[device], :] = np.nan
            d['sem2DRaw'] = d['sem2DRaw'][:self._nChannelsMax,:]
            position = (np.arange(self._nChannelsMax)-self._nChannelsMax/2)*2.17
            if self.mode == '2D':   
                ax.pcolormesh(position, d['measStamp'], 
                    d['sem2DRaw'][:, :n_meas_eff].T, shading='nearest')
            elif self.mode == '1D':
                cmap = matplotlib.cm.get_cmap('viridis')
                for ii in range(n_meas_eff):
                    c = cmap(float(ii)/n_meas_eff)
                    ax.plot(position,-d['sem2DRaw'][:, ii],c=c)
            else:
                print('WARNING, mode=',self.mode,' not understood')
            title_str = device.split('/')[0] + self.liner[device]
            ax.set_title(title_str + '\n' + self.generateTitleStr(data[device]), fontsize=10)
            ax.xaxis.set_tick_params(labelbottom=True)
            ax.yaxis.set_tick_params(labelleft=True) #
            ax.set_xlabel('Position (mm)')
        ax.set_ylim(bottom=-0.02*ax.get_ylim()[1])
        if self.mode == '2D':
            [ax.set_ylabel('Cycle time (ms)') for ax in self.axs[:,0]]
        elif self.mode == '1D':
            [ax.set_ylabel('e-cloud signal (a.u.)') for ax in self.axs.flatten()]#[:,0]]
        plt.tight_layout()
        self.drawFigure()
    '''

    def plot(self, data):
        if not plt.fignum_exists(self.figname):
            return
        self.clearFigure()
        figure, axs = self.createSubplots2()
        for j, device in enumerate(self.devices):
            d = data[device]['value']
            n_channels, n_meas_2d = d['sem2DRaw'].shape
            n_meas_eff = d['nbOfMeas']
            signal = d['sem2DRaw'].astype(np.float) / d['totalGain'] 
            signal[self.deadChannels[device], :] = np.nan
            signal *= self.signalInversion[device]
            signal = signal[:self._nChannelsMax,:]
            signal_sum = np.nansum(signal,axis=0)
            position = (np.arange(self._nChannelsMax)-self._nChannelsMax/2)*2.17
            ax = axs.T.flatten()[j*2+1]
            axt = axs.T.flatten()[j*2]
            if self.mode == '2D':   
                ax.pcolormesh(position, d['measStamp'], 
                    signal[:, :n_meas_eff].T, shading='nearest')
                ax.set_ylabel('Cycle time (ms)')
            elif self.mode == '1D':
                cmap = matplotlib.cm.get_cmap('viridis')
                for ii in range(n_meas_eff):
                    c = cmap(float(ii)/n_meas_eff)
                    ax.plot(position,signal[:, ii],c=c)
                ax.set_ylabel('e-cloud signal (a.u.)')
            else:
                print('WARNING, mode=',self.mode,' not understood')
            axt.plot(d['measStamp'],signal_sum[:n_meas_eff],'r')
            axt.set_xlabel('Cycle time (ms)')
            axt.set_ylabel('e-cloud signal \nintegrated (a.u.)')
            title_str = device.split('/')[0] + self.liner[device]
            axt.set_title(title_str + '\n' + self.generateTitleStr(data[device]), fontsize=10)
            ax.xaxis.set_tick_params(labelbottom=True)
            ax.yaxis.set_tick_params(labelleft=True) #
            ax.set_xlabel('Position (mm)')
        ax.set_ylim(bottom=-0.02*ax.get_ylim()[1])
        self.drawFigure()


class ABWLM(PlottingClassSPS):

    def __init__(self, device='ABWLMSPS/Acquisition', figsize=(7, 9), *args, **kw):
        super().__init__(figsize,*args,**kw)
        self._nrows = 2
        self._sharex = True
        self.initializeFigure()
        self.device = device

    def plot(self, data):
        if not plt.fignum_exists(self.figname):
            return
        self.clearFigure()
        figure, axs = self.createSubplots()
        d = data[self.device]['value']
        time_vect = d['acqTimeFromInj']
        axs[0].plot(time_vect,1e9*d['bunchLengths'])
        axs[0].set_title(self.generateTitleStr(data[self.device]),fontsize=10)
        n_meas, n_bunches = d['bunchIntensities'].shape
        bunchslot_vect = np.arange(n_bunches)
        #axs[1].pcolormesh(time_vect, bunchslot_vect, d['bunchIntensities'].T, shading='nearest')
        axs[1].plot(time_vect,d['bunchIntensities'])
        self.axs[1].set_xlabel('Cycle time (ms)')
        self.axs[0].set_ylabel('Bunch length (ns)')
        self.axs[1].set_ylabel('Bunch intensity (arbitrary units)')
        plt.tight_layout()
        self.drawFigure()


class LHCBPM(PlottingClassSPS):

    def __init__(self, device='BPLOFSBA5/GetCapData', 
            bpmSelection=['SPS.BPMB.51303','SPS.BPMB.51503','SPS.BPMB.51999'],
            mode='turnByTurn',*args,**kw):
        figsize = (5*len(bpmSelection),6)
        super().__init__(figsize,*args,**kw)
        self._nrows = 2
        self._ncols = len(bpmSelection)
        self._sharex = True
        self._sharey = True
        self.initializeFigure()
        self.device = device
        self.bpmSelection = bpmSelection
        self.mode = mode

    def plot(self, data):
        if not plt.fignum_exists(self.figname):
            return
        self.clearFigure()
        figure, axs = self.createSubplots()
        d = self.reshapeData(data[self.device]['value'])
        cmap = matplotlib.cm.get_cmap('Spectral_r')
        for jj,bpm in enumerate(self.bpmSelection):
            bpm_sel = d['bpmNames'].tolist().index(bpm)
            for ii,par in enumerate(['horPosition','verPosition']):
                msk = np.where(np.std(d[par][bpm_sel],axis=0))[0]
                ax = axs[ii,jj]
                ax.text(0.5,0.9,bpm,ha='center',va='bottom',transform=ax.transAxes)
                if self.mode == 'turnByTurn':
                    ax.plot(d[par][bpm_sel][:,msk])               
                    axs[1,jj].set_xlabel('turns')
                elif self.mode == 'bunchByBunch':
                    bunchId = d[par[:3]+'BunchId'][bpm_sel]
                    for i in range(d['nbOfCapTurns']):
                        c=cmap(float(i)/d['nbOfCapTurns'])
                        ax.plot(bunchId[msk],d[par][bpm_sel][i,msk].T,color=c)
                    axs[1,jj].set_xlabel('bunch #')
                label=par.replace('horP','horizontal p').replace('verP','vertical p')
                axs[ii,0].set_ylabel(label + ' (mm)')
        self.figure.suptitle(self.generateTitleStr(data[self.device]), fontsize=10)
        plt.tight_layout()
        self.drawFigure()

    def reshapeData(self,data):
        nbOfCapBunches = data['nbOfCapBunches']
        nbOfCapTurns = data['nbOfCapTurns']
        for par in ['horPosition','verPosition']:
            data[par]=[pos.reshape(nbOfCapTurns,nbOfCapBunches) for pos in data[par]]
        return data


class BWS(PlottingClassSPS):

    def __init__(self,device,pmSelection,beta_func,disp_func=0,dpp=[0,0],resultPath=None,
           n_sigma_plot=8,figsize=(10,7),*args,**kw):
        super().__init__(figsize,*args,**kw)
        self.figname = self.__class__.__name__ + ': ' + device.split('/')[0] + \
            ' - pmSelection: %d'%pmSelection 
        self._nrows = 2
        self._ncols = 2
        self.initializeFigure()
        self.device = device
        self.pmSelection = pmSelection
        self.beta_func = beta_func
        self.disp_func = disp_func
        self.dpp = dpp
        self.resultPath = resultPath
        self.n_sigma_plot = n_sigma_plot


    def plot(self, data):
        if not plt.fignum_exists(self.figname):
            return
        if not data[self.device]['value']:
            return
        d = data[self.device]['value']
        pmSelectionAcq = d['pmtSelection']['JAPC_ENUM']['code']
        if not ((pmSelectionAcq == self.pmSelection) or (pmSelectionAcq==5)):
            return
        self.clearFigure()
        figure, axs = self.createSubplots()
        gamma_cycle = data['SPSBEAM/GAMMA']['value']['JAPC_FUNCTION']
        popt, emit = {}, {}

        self.lastResult = {}
        lastResult = self.lastResult
        lastResult['device'] = self.device
        lastResult['cycleStamp'] = data[self.device]['header']['cycleStamp']
        lastResult['PMselection'] = self.pmSelection
        lastResult['beta_func'] = self.beta_func
        lastResult['disp_func'] = self.disp_func

        for ii,ws_set in enumerate(['Set1','Set2']):
            delay = d['delays'][ii]/1e3
            gamma = np.interp(delay,gamma_cycle['X'],gamma_cycle['Y'])
            betagamma = wsa.betagamma(gamma)
            pos_all,prof_all = wsa.getProfileData(d,ws_set)
            popt[ws_set] = []
            for pos,prof in zip(pos_all,prof_all[self.pmSelection-1]):
                popt[ws_set].append(wsa.fitGauss(pos,prof))
                l=axs[ii,0].plot(pos,prof,lw=0.5)[0]
                axs[ii,0].plot(pos,wsa.myGauss5p(pos,*popt[ws_set][-1]),c=l.get_color())
            #axs[ii,0].set_xlabel('position (mm)')
            axs[ii,0].set_ylabel('amplitude (a.u.)')
            amplitude = np.array(popt[ws_set])[:,0]
            sigma_raw = np.array(popt[ws_set])[:,2]
            sigma_bet = 1e3*np.sqrt((sigma_raw/1e3)**2 - (self.dpp[ii]*self.disp_func)**2)
            emit[ws_set] = wsa.getEmittance(sigma_bet,self.beta_func,betagamma)
            area = sigma_raw*amplitude
            msk = area>0.4*np.nanmax(area)
            if not sum(msk):
                print(' *** WARNING *** no bunch detected')
                print(sigma_raw)
                return
            emit_mean = np.mean(emit[ws_set][msk])
            emit_std = np.std(emit[ws_set][msk])
            if abs(self.disp_func)>1e-3:
                dpp_str = 'dpp: %1.2fe-3'%(self.dpp[ii]*1e3)
            else:
                dpp_str = ''
            axs[ii,1].bar(d['bunchSelection'][msk],emit[ws_set][msk],label='emittance')
            axs[ii,1].bar(d['bunchSelection'], area/max(area)*min(emit[ws_set][msk])*0.6,
                alpha=0.4, label='profile area (a.u.)')
            axs[ii,1].text(0.02,0.97,'avg. of %d bunches: %1.2f um'%(sum(msk),emit_mean) + 
                '\nstd. of %d bunches: %1.2f um'%(sum(msk),emit_std),
                transform=axs[ii,1].transAxes,va='top')
            axs[ii,0].text(0.02,0.97,'acquisition @ %d ms'%(delay) + 
                '\nPMT selected: %d'%(self.pmSelection) + 
                '\nPMT best: %d'%(d['bestChannel'+ws_set]),
                transform=axs[ii,0].transAxes,va='top',ha='left')
            axs[ii,1].set_ylabel('emittance (um)')
            axs[ii,1].legend(loc='upper right')
            # to set xlims on profile plot
            sigma_max = np.nanmax(sigma_raw[msk])
            mu_mean = np.nanmean(np.array(popt[ws_set])[msk,1])
            new_xlim = mu_mean+sigma_max*self.n_sigma_plot*np.array([-1,1])
            if new_xlim[0]>axs[ii,0].get_xlim()[0]:
                axs[ii,0].set_xlim(left=new_xlim[0])
            if new_xlim[1]<axs[ii,0].get_xlim()[1]:
                axs[ii,0].set_xlim(right=new_xlim[1])
            axs[ii,0].set_xlabel('position (mm)')
            axs[ii,1].set_xlabel('bunch #')
            
            lastResult['emittance_%s'%ws_set] = emit[ws_set]*1e-6
            lastResult['emittance_average_%s'%ws_set] = emit_mean*1e-6
            lastResult['emittance_std_%s'%ws_set] = emit_std*1e-6
            lastResult['sigma_%s'%ws_set] = sigma_raw*1e-3
            lastResult['sigma_betatronic_%s'%ws_set] = sigma_bet*1e-3
            #lastResult['intensity_%s'%ws_set] = intensity*1e10
            lastResult['acq_time_%s'%ws_set] = delay
            lastResult['dpp_%s'%ws_set] = self.dpp[ii]
            lastResult['mask_%s'%ws_set] = msk
            lastResult['bunchSelection_%s'%ws_set] = d['bunchSelection']
            lastResult['betagamma_%s'%ws_set] = betagamma
            lastResult['bestChannel_%s'%ws_set] = d['bestChannel' + ws_set]

        axs[0,1].sharex(axs[1,1])
        sharey = True
        for ax in axs[:,1]:
            if ax.get_ylim()[1]>20: 
                sharey=False
        if sharey:
            axs[0,1].sharey(axs[1,1])
            axs[0,1].set_ylim(0,1.4*axs[0,1].get_ylim()[1])
        else:
            [ax.set_ylim(0,1.4*ax.get_ylim()[1]) for ax in axs[:,1]]
        self.figure.suptitle(self.device.split('/')[0] + ' - ' +
            self.generateTitleStr(data[self.device]), fontsize=10)
        plt.tight_layout()
        plt.subplots_adjust(wspace=0.15)
        self.drawFigure()
        #if lastResult['cycleStamp'] > 0:
        #    self.results_to_file()

    def results_to_file(self):
        ts_str = datetime.fromtimestamp(self.lastResult['cycleStamp']/1e9).strftime('%Y.%m.%d.%H.%M.%S.%f')
        if self.resultPath:

            if not os.path.exists(self.resultPath):
                os.makedirs(self.resultPath)

            filename = self.resultPath+ts_str+'_PM'+str(self.pmSelection)+'.parquet'

            lastResults_pd = ds.dict_to_pandas(self.lastResult)
            lastResults_pd.to_parquet(filename)

            print(f'   Saving results to file {filename}.')


class MR(PlottingClassSPS):

    def __init__(self, device='SMR.SCOPE13.CH01/Acquisition', figsize=(6.5, 7), *args, **kw):
        super().__init__(figsize, *args, **kw)
        self.initializeFigure()
        self.device = device
        self._nrows = 2
        self._sharex = True

    def plot(self, data):
        if not plt.fignum_exists(self.figname):
            return
        self.clearFigure()
        figure, axs = self.createSubplots()
        d = data[self.device]['value']
        n_meas, n_samples = d['value'].shape
        time_vect = np.tile( (d['triggerStamp'] - d['triggerStamp'][0]) / 1e3, (n_samples,1)).T
        sample_vect = np.arange(n_samples)*d['sampleInterval'] + d['firstSampleTime']
        sample_vect = sample_vect.reshape(1,n_samples) + d['triggerError'].reshape(n_meas,1)
        signal = d['value']*d['sensitivity']
        axs[0].contourf(sample_vect, time_vect, signal, levels=100, cmap='hot_r')
        offset = np.max(signal)/n_meas*3
        for i, (sample_vect, trace) in enumerate(zip(sample_vect, signal)):
            axs[1].plot(sample_vect,trace+i*offset,lw=0.4)
        axs[0].set_title(self.generateTitleStr(data[self.device]), fontsize=10)
        axs[0].set_ylabel('Time (us)')
        axs[1].set_ylabel('Signal (V)')
        axs[1].set_xlabel('Time (ns)')
        plt.tight_layout()
        self.drawFigure()

